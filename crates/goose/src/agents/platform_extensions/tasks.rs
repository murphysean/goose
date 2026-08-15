pub mod handlers;
pub mod process;

#[cfg(test)]
mod tests;

use crate::agents::extension::PlatformExtensionContext;
use crate::agents::mcp_client::{Error, McpClientTrait};
use crate::agents::tool_execution::ToolCallContext;
use anyhow::Result;
use async_trait::async_trait;
pub use process::{
    format_duration, parse_duration, read_from_position, tail_lines, write_input_to_process,
    ManagedProcess,
};
use rmcp::model::{
    CallToolResult, ContentBlock, Implementation, InitializeResult, JsonObject, ListToolsResult,
    ServerCapabilities, Tool,
};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub static EXTENSION_NAME: &str = "tasks";

pub struct TasksClient {
    info: InitializeResult,
    context: PlatformExtensionContext,
    pub processes: Arc<Mutex<HashMap<String, ManagedProcess>>>,
    pub next_id: Arc<AtomicU64>,
}

impl TasksClient {
    pub fn new(context: PlatformExtensionContext) -> Result<Self> {
        let info = InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new(EXTENSION_NAME, "1.0.0").with_title("Task Manager"),
            )
            .with_instructions(
                "Unified task management for processes, timers, and subagents. \
                 Use start_task to create any kind of task, send_input to send data or signals, \
                 read_output to read results, and cancel_task/list_tasks/get_task to manage lifecycle.",
            );

        let processes: Arc<Mutex<HashMap<String, ManagedProcess>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let next_id: Arc<AtomicU64> = Arc::new(AtomicU64::new(1));

        // Start background cleanup/GC task for task inactivity TTL (5 mins = 300s)
        let processes_for_gc = Arc::clone(&processes);
        let registry_for_gc = context.task_registry.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                let mut procs = processes_for_gc.lock().await;
                let mut reg = registry_for_gc.lock().await;
                let mut to_remove = Vec::new();
                for (id, _) in procs.iter() {
                    let mut inactive = false;
                    if let Some(task) = reg.get(id) {
                        if task.last_activity.elapsed() >= std::time::Duration::from_secs(300) {
                            inactive = true;
                        }
                    } else {
                        // Already reaped/removed from registry
                        inactive = true;
                    }
                    if inactive {
                        to_remove.push(id.clone());
                    }
                }
                for id in to_remove {
                    if let Some(proc) = procs.remove(&id) {
                        match proc {
                            ManagedProcess::Pipe {
                                mut child,
                                stdout_path,
                                stderr_path,
                                ..
                            } => {
                                let _ = child.kill().await;
                                let _ = child.wait().await;
                                let _ = tokio::fs::remove_file(stdout_path).await;
                                let _ = tokio::fs::remove_file(stderr_path).await;
                            }
                            ManagedProcess::Pty {
                                mut child,
                                stdout_path,
                                ..
                            } => {
                                child.start_kill().ok();
                                child.wait().await.ok();
                                let _ = tokio::fs::remove_file(stdout_path).await;
                            }
                        }
                        reg.remove(&id);
                    }
                }
            }
        });

        Ok(Self {
            info,
            context,
            processes,
            next_id,
        })
    }

    pub async fn next_task_id(&self) -> String {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("task_{}", id)
    }
}

// ── Tool registration ──────────────────────────────────────────────────

#[async_trait]
impl McpClientTrait for TasksClient {
    async fn list_tools(
        &self,
        _session_id: &str,
        _next_cursor: Option<String>,
        _cancel_token: CancellationToken,
    ) -> Result<ListToolsResult, Error> {
        let tools = vec![
            Tool::new(
                "start_task",
                "Create a new background task. Supports kinds: 'process' (run a shell command), 'timer' (schedule a timed reminder), 'subagent' (delegate work to a subagent — use the delegate tool instead). Returns a task_id for monitoring.".to_string(),
                serde_json::json!({
                    "type": "object",
                    "required": ["kind"],
                    "properties": {
                        "kind": {
                            "type": "string",
                            "description": "Task kind: 'process' (shell command), 'timer' (timed reminder), or 'subagent' (use delegate tool instead)"
                        },
                        "command": {
                            "type": "string",
                            "description": "Shell command to run (for kind: 'process')"
                        },
                        "use_pty": {
                            "type": "boolean",
                            "description": "Use a PTY for interactive programs (python3, gdb, ssh). Default: false."
                        },
                        "wait_for": {
                            "type": "string",
                            "description": "Pattern to watch for in stdout. When matched, triggers a notification. Process keeps running."
                        },
                        "timeout_seconds": {
                            "type": "number",
                            "description": "Kill the process after this many seconds."
                        },
                        "working_dir": {
                            "type": "string",
                            "description": "Working directory for the process."
                        },
                        "env": {
                            "type": "object",
                            "description": "Environment variables for the process.",
                            "additionalProperties": { "type": "string" }
                        },
                        "delay": {
                            "type": "string",
                            "description": "Timer delay (for kind: 'timer'). Examples: '30s', '5m', '2h', '1h30m'"
                        },
                        "message": {
                            "type": "string",
                            "description": "Reminder message (for kind: 'timer'). Injected when the timer fires."
                        }
                    }
                }).as_object().unwrap().clone(),
            ),
            Tool::new(
                "send_input",
                "Send input to a running task. Supports text input (auto-appends newline), raw bytes (no newline), Unix signals, and responding to tasks waiting for input. Use wait_for to block until a pattern appears in output.".to_string(),
                serde_json::json!({
                    "type": "object",
                    "required": ["task_id"],
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to send input to"
                        },
                        "input": {
                            "type": "string",
                            "description": "Text to send. Newline is auto-appended. Set no_enter: true to suppress."
                        },
                        "bytes": {
                            "type": "array",
                            "description": "Raw bytes to send (0-255). No newline appended. Use for control characters: [3]=Ctrl-C, [1,24]=Ctrl-A Ctrl-X, [9]=Tab.",
                            "items": { "type": "integer", "minimum": 0, "maximum": 255 }
                        },
                        "signal": {
                            "type": "string",
                            "description": "Unix signal to send instead of text. Examples: SIGINT (Ctrl-C), SIGTERM, SIGKILL, SIGSTOP, SIGCONT, SIGHUP, SIGQUIT."
                        },
                        "no_enter": {
                            "type": "boolean",
                            "description": "Suppress the automatic newline after text input. Default: false."
                        },
                        "close_stdin": {
                            "type": "boolean",
                            "description": "Close stdin after writing (signal EOF). Needed for interactive programs like python3 that buffer output until stdin closes. Default: false."
                        },
                        "elicit": {
                            "type": "boolean",
                            "description": "Prompt the user for input (for passwords/secrets). Default: false."
                        },
                        "elicit_message": {
                            "type": "string",
                            "description": "Custom prompt message for elicitation (e.g. 'Enter the sudo password')"
                        },
                        "wait_for": {
                            "type": "string",
                            "description": "Wait for this pattern in output before returning. Use shell/REPL prompts like '$', '>>>', '(gdb)'. Max 30s wait."
                        }
                    }
                }).as_object().unwrap().clone(),
            ),
            Tool::new(
                "read_output",
                "Read output from a running process task. Supports waiting for patterns and timeouts. Output is automatically stripped of ANSI codes by default.".to_string(),
                serde_json::json!({
                    "type": "object",
                    "required": ["task_id"],
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to read output from"
                        },
                        "wait_for": {
                            "type": "string",
                            "description": "Wait until this pattern appears in output before returning. Useful for waiting for prompts like '$', '>>>', 'Build successful'."
                        },
                        "timeout_ms": {
                            "type": "number",
                            "description": "Maximum time to wait for output in milliseconds. Default: 30000 (30s)."
                        },
                        "strip_ansi": {
                            "type": "boolean",
                            "description": "Strip ANSI escape codes from output. Default: true."
                        },
                        "limit_lines": {
                            "type": "integer",
                            "description": "Limit output to the last X lines. If specified, does not update or use the incremental read offset."
                        }
                    }
                }).as_object().unwrap().clone(),
            ),
            Tool::new(
                "list_tasks",
                "List all active background tasks (processes, timers, subagents) with their current status.".to_string(),
                serde_json::json!({
                    "type": "object",
                    "properties": {}
                }).as_object().unwrap().clone(),
            ),
            Tool::new(
                "cancel_task",
                "Cancel a running background task by its task ID. Kills the OS process if it's a process task. Works for any task type.".to_string(),
                serde_json::json!({
                    "type": "object",
                    "required": ["task_id"],
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to cancel"
                        }
                    }
                }).as_object().unwrap().clone(),
            ),
            Tool::new(
                "get_task",
                "Retrieve and reap a completed background task by its task ID. Returns the task's output summary and status, then removes it from the task registry.".to_string(),
                serde_json::json!({
                    "type": "object",
                    "required": ["task_id"],
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to read"
                        }
                    }
                }).as_object().unwrap().clone(),
            ),
        ];

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        ctx: &ToolCallContext,
        name: &str,
        arguments: Option<JsonObject>,
        _cancel_token: CancellationToken,
    ) -> Result<CallToolResult, Error> {
        let result = match name {
            "start_task" => self.handle_start_task(arguments).await,
            "send_input" => self.handle_send_input(ctx, arguments).await,
            "read_output" => self.handle_read_output(ctx, arguments).await,
            "list_tasks" => self.handle_list_tasks().await,
            "cancel_task" => self.handle_cancel_task(arguments).await,
            "get_task" => self.handle_get_task(arguments).await,
            _ => Err(format!("Unknown tool: {}", name)),
        };

        match result {
            Ok(r) => Ok(r),
            Err(e) => Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "Error: {}",
                e
            ))])),
        }
    }

    fn get_info(&self) -> Option<&InitializeResult> {
        Some(&self.info)
    }
}
