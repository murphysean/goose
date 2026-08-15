use super::*;
use crate::agents::tool_execution::ToolCallContext;
use crate::tasks::{NotifyPolicy, Task, TaskSource, TaskState};
use anyhow::Result;
use rmcp::model::{CallToolResult, ContentBlock, JsonObject};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

impl TasksClient {
    // ── start_task ──────────────────────────────────────────────────────

    pub async fn handle_start_task(
        &self,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, String> {
        let args = arguments.ok_or("Missing arguments")?;
        let kind = args
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or("Missing 'kind' argument (must be 'process', 'timer', or 'subagent')")?;

        match kind {
            "process" => self.start_process(args).await,
            "timer" => self.start_timer(args).await,
            "subagent" => self.start_subagent(args).await,
            other => Err(format!(
                "Unknown task kind '{}'. Must be 'process', 'timer', or 'subagent'.",
                other
            )),
        }
    }

    async fn start_process(&self, args: JsonObject) -> Result<CallToolResult, String> {
        let command = args
            .get("command")
            .and_then(|v| v.as_str())
            .ok_or("Missing 'command' argument")?;
        let wait_for = args
            .get("wait_for")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let use_pty = args
            .get("use_pty")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let timeout_seconds = args.get("timeout_seconds").and_then(|v| v.as_u64());

        let id = self.next_task_id().await;

        if use_pty {
            return self
                .start_pty_process(&id, command, &args, wait_for, timeout_seconds)
                .await;
        }

        let stdout_path = format!("/tmp/goose_task_{}_stdout.log", id);
        let stderr_path = format!("/tmp/goose_task_{}_stderr.log", id);

        // Pre-create empty logs
        let _ = tokio::fs::File::create(&stdout_path).await;
        let _ = tokio::fs::File::create(&stderr_path).await;

        let mut child = Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to start process: {}", e))?;

        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        let (stdout_done_tx, stdout_done_rx) = tokio::sync::oneshot::channel::<()>();

        if let Some(stdout) = stdout {
            let pattern = wait_for.clone();
            let registry = self.context.task_registry.clone();
            let task_id = id.clone();
            let path = stdout_path.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout);
                let mut file = match tokio::fs::File::create(&path).await {
                    Ok(f) => f,
                    Err(_) => return,
                };
                let mut line = String::new();
                let mut pattern_fired = false;
                while let Ok(n) = reader.read_line(&mut line).await {
                    if n == 0 {
                        break;
                    }
                    let _ = file.write_all(line.as_bytes()).await;
                    let _ = file.flush().await;

                    if !pattern_fired {
                        if let Some(ref pat) = pattern {
                            if line.contains(pat.as_str()) {
                                pattern_fired = true;
                                registry.lock().await.pattern_matched(
                                    &task_id,
                                    pat.clone(),
                                    line.clone(),
                                );
                            }
                        }
                    }
                    line.clear();
                }
                let _ = stdout_done_tx.send(());
            });
        } else {
            let _ = stdout_done_tx.send(());
        }

        if let Some(stderr) = stderr {
            let path = stderr_path.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut file = match tokio::fs::File::create(&path).await {
                    Ok(f) => f,
                    Err(_) => return,
                };
                let mut line = String::new();
                while let Ok(n) = reader.read_line(&mut line).await {
                    if n == 0 {
                        break;
                    }
                    let _ = file.write_all(line.as_bytes()).await;
                    let _ = file.flush().await;
                    line.clear();
                }
            });
        }

        // Completion watcher
        let registry = Arc::clone(&self.context.task_registry);
        let task_id = id.clone();
        let processes = Arc::clone(&self.processes);
        tokio::spawn(async move {
            let _ = stdout_done_rx.await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let code = {
                let mut procs = processes.lock().await;
                if let Some(ManagedProcess::Pipe { ref mut child, .. }) = procs.get_mut(&task_id) {
                    child.try_wait().ok().flatten().and_then(|s| s.code())
                } else {
                    None
                }
            };
            let mut reg = registry.lock().await;
            if let Some(task) = reg.get(&task_id) {
                if !task.state.is_terminal() {
                    if code.unwrap_or(0) == 0 {
                        reg.complete(&task_id, None);
                    } else {
                        reg.fail(&task_id, Some(format!("exit code: {}", code.unwrap_or(-1))));
                    }
                }
            }
        });

        // Timeout watcher
        if let Some(timeout) = timeout_seconds {
            let registry = Arc::clone(&self.context.task_registry);
            let processes = Arc::clone(&self.processes);
            let task_id = id.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(timeout)).await;
                let mut reg = registry.lock().await;
                if let Some(task) = reg.get(&task_id) {
                    if !task.state.is_terminal() {
                        reg.fail(&task_id, Some("Timed out".to_string()));
                    }
                }
                drop(reg);
                let mut procs = processes.lock().await;
                if let Some(ManagedProcess::Pipe {
                    mut child,
                    stdout_path,
                    stderr_path,
                    ..
                }) = procs.remove(&task_id)
                {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    let _ = tokio::fs::remove_file(stdout_path).await;
                    let _ = tokio::fs::remove_file(stderr_path).await;
                }
            });
        }

        let description = if command.len() > 60 {
            format!("{}...", &command.chars().take(60).collect::<String>())
        } else {
            command.to_string()
        };

        // Register in task registry
        let registry = Arc::clone(&self.context.task_registry);
        let task = Task {
            id: id.clone(),
            source: TaskSource::Process,
            description: description.clone(),
            state: TaskState::Working,
            batch_id: None,
            notify_policy: NotifyPolicy::OnCompletion,
            meta: crate::tasks::TaskMeta::default(),
            created_at: std::time::Instant::now(),
            last_activity: std::time::Instant::now(),
            worked_duration: std::time::Duration::default(),
            notifications: Vec::new(),
            status_message: None,
            result: None,
            error: None,
            result_summary: None,
            input_request: None,
            ttl_ms: Some(300_000), // 5 min default TTL
            poll_interval_ms: None,
            cancellation_token: None,
        };
        registry.lock().await.register(task);

        self.processes.lock().await.insert(
            id.clone(),
            ManagedProcess::Pipe {
                child,
                stdin,
                stdout_path,
                stderr_path,
                stdout_pos: 0,
                stderr_pos: 0,
            },
        );

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Task {} started (process). TTL: 5 minutes of inactivity before automatic cleanup.\nCommand: {}\n\n\
             Use send_input(task_id: \"{}\", input: \"...\") to send input, \
             read_output(task_id: \"{}\") to check output, cancel_task(task_id: \"{}\") to stop.",
            id, command, id, id, id
        ))]))
    }

    async fn start_pty_process(
        &self,
        id: &str,
        command: &str,
        args: &JsonObject,
        wait_for: Option<String>,
        timeout_seconds: Option<u64>,
    ) -> Result<CallToolResult, String> {
        let (pty, pts) = pty_process::open().map_err(|e| format!("Failed to open PTY: {}", e))?;
        pty.resize(pty_process::Size::new(24, 80))
            .map_err(|e| format!("Failed to resize PTY: {}", e))?;

        let mut cmd = pty_process::Command::new("sh");
        cmd = cmd.arg("-c").arg(command);
        if let Some(dir) = args.get("working_dir").and_then(|v| v.as_str()) {
            cmd = cmd.current_dir(dir);
        }
        if let Some(env) = args.get("env").and_then(|v| v.as_object()) {
            for (k, v) in env {
                if let Some(val) = v.as_str() {
                    cmd = cmd.env(k, val);
                }
            }
        }

        let child = cmd
            .spawn(pts)
            .map_err(|e| format!("Failed to spawn PTY process: {}", e))?;

        let (read_pty, write_pty) = pty.into_split();
        let stdout_path = format!("/tmp/goose_task_{}_stdout.log", id);

        // Pre-create empty log
        let _ = tokio::fs::File::create(&stdout_path).await;

        let pattern = wait_for.clone();
        let registry = self.context.task_registry.clone();
        let task_id = id.to_string();
        let path = stdout_path.clone();

        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut reader = read_pty;
            let mut file = match tokio::fs::File::create(&path).await {
                Ok(f) => f,
                Err(_) => return,
            };
            let mut buf = vec![0u8; 4096];
            let mut line_buf = String::new();
            let mut pattern_fired = false;
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let _ = file.write_all(&buf[..n]).await;
                        let _ = file.flush().await;

                        let chunk = String::from_utf8_lossy(&buf[..n]);
                        line_buf.push_str(&chunk);
                        #[allow(clippy::string_slice)]
                        while let Some(pos) = line_buf.find('\n') {
                            let line = line_buf[..pos].to_string();
                            line_buf.drain(..=pos);
                            let clean = line.replace('\r', "");
                            if !pattern_fired {
                                if let Some(ref pat) = pattern {
                                    if clean.contains(pat.as_str()) {
                                        pattern_fired = true;
                                        registry.lock().await.pattern_matched(
                                            &task_id,
                                            pat.clone(),
                                            clean,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let pty_writer = Arc::new(tokio::sync::Mutex::new(write_pty));

        // Completion watcher
        let registry = Arc::clone(&self.context.task_registry);
        let task_id = id.to_string();
        let processes = Arc::clone(&self.processes);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let mut procs = processes.lock().await;
            if let Some(ManagedProcess::Pty { child, .. }) = procs.get_mut(&task_id) {
                if let Ok(Some(status)) = child.try_wait() {
                    let code = status.code();
                    drop(procs);
                    let mut reg = registry.lock().await;
                    if let Some(task) = reg.get(&task_id) {
                        if !task.state.is_terminal() {
                            if code.unwrap_or(0) == 0 {
                                reg.complete(&task_id, None);
                            } else {
                                reg.fail(
                                    &task_id,
                                    Some(format!("exit code: {}", code.unwrap_or(-1))),
                                );
                            }
                        }
                    }
                }
            }
        });

        // Timeout watcher
        if let Some(timeout) = timeout_seconds {
            let registry = Arc::clone(&self.context.task_registry);
            let processes = Arc::clone(&self.processes);
            let task_id = id.to_string();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(timeout)).await;
                let mut reg = registry.lock().await;
                if let Some(task) = reg.get(&task_id) {
                    if !task.state.is_terminal() {
                        reg.fail(&task_id, Some("Timed out".to_string()));
                    }
                }
                drop(reg);
                let mut procs = processes.lock().await;
                if let Some(ManagedProcess::Pty {
                    mut child,
                    stdout_path,
                    ..
                }) = procs.remove(&task_id)
                {
                    child.start_kill().ok();
                    child.wait().await.ok();
                    let _ = tokio::fs::remove_file(stdout_path).await;
                }
            });
        }

        let description = if command.len() > 60 {
            format!("{}...", &command.chars().take(60).collect::<String>())
        } else {
            command.to_string()
        };

        // Register in task registry
        let registry = Arc::clone(&self.context.task_registry);
        let task = Task {
            id: id.to_string(),
            source: TaskSource::Process,
            description: description.clone(),
            state: TaskState::Working,
            batch_id: None,
            notify_policy: NotifyPolicy::OnCompletion,
            meta: crate::tasks::TaskMeta::default(),
            created_at: std::time::Instant::now(),
            last_activity: std::time::Instant::now(),
            worked_duration: std::time::Duration::default(),
            notifications: Vec::new(),
            status_message: None,
            result: None,
            error: None,
            result_summary: None,
            input_request: None,
            ttl_ms: Some(300_000), // 5 min default TTL
            poll_interval_ms: None,
            cancellation_token: None,
        };
        registry.lock().await.register(task);

        self.processes.lock().await.insert(
            id.to_string(),
            ManagedProcess::Pty {
                child,
                pty_writer,
                stdout_path,
                stdout_pos: 0,
            },
        );

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Task {} started (process, PTY mode). TTL: 5 minutes of inactivity before automatic cleanup.\nCommand: {}\n\n\
             Use send_input(task_id: \"{}\", input: \"...\") to send input, \
             read_output(task_id: \"{}\") to check output, cancel_task(task_id: \"{}\") to stop.",
            id, command, id, id, id
        ))]))
    }

    async fn start_timer(&self, args: JsonObject) -> Result<CallToolResult, String> {
        let delay_str = args
            .get("delay")
            .and_then(|v| v.as_str())
            .ok_or("Missing 'delay' argument (e.g. '30s', '5m', '2h')")?;
        let message = args
            .get("message")
            .and_then(|v| v.as_str())
            .ok_or("Missing 'message' argument")?;

        let delay = parse_duration(delay_str)?;
        let id = self.next_task_id().await;

        let registry = Arc::clone(&self.context.task_registry);
        let task_id = id.clone();
        let msg = message.to_string();

        {
            let mut reg = registry.lock().await;
            reg.register(Task {
                id: task_id.clone(),
                source: TaskSource::Timer,
                description: format!("Timer: {} ({})", message, delay_str),
                state: TaskState::Working,
                batch_id: None,
                notify_policy: NotifyPolicy::OnCompletion,
                meta: crate::tasks::TaskMeta::default(),
                created_at: std::time::Instant::now(),
                last_activity: std::time::Instant::now(),
                worked_duration: std::time::Duration::default(),
                notifications: Vec::new(),
                status_message: None,
                result: None,
                error: None,
                result_summary: None,
                input_request: None,
                ttl_ms: Some(300_000), // 5 min default TTL
                poll_interval_ms: None,
                cancellation_token: None,
            });
        }

        tokio::spawn(async move {
            let _ = tokio::time::sleep(delay).await;
            let mut reg = registry.lock().await;
            reg.complete(&task_id, Some(msg));
        });

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Task {} started (timer)\nReminder set for {}: {}\n\nThe system will notify you when the timer fires.",
            id, delay_str, message
        ))]))
    }

    async fn start_subagent(&self, _args: JsonObject) -> Result<CallToolResult, String> {
        Err(
            "Subagent tasks should be created via the delegate tool in the summon extension. \
             Use delegate(source: \"...\", instructions: \"...\", async: true) to run a subagent \
             in the background."
                .to_string(),
        )
    }

    // ── send_input ──────────────────────────────────────────────────────

    pub async fn handle_send_input(
        &self,
        ctx: &ToolCallContext,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, String> {
        let args = arguments.ok_or("Missing arguments")?;
        let task_id = args
            .get("task_id")
            .and_then(|v| v.as_str())
            .ok_or("Missing 'task_id' argument")?;

        // Update task activity
        if let Some(task) = self.context.task_registry.lock().await.get_mut(task_id) {
            task.last_activity = std::time::Instant::now();
        }

        // Check if this is a signal
        if let Some(signal) = args.get("signal").and_then(|v| v.as_str()) {
            return self.send_signal_to_task(task_id, signal).await;
        }

        // Check if this is a raw bytes input
        if let Some(bytes) = args.get("bytes").and_then(|v| v.as_array()) {
            return self.send_bytes_to_task(task_id, bytes).await;
        }

        let elicit = args
            .get("elicit")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if elicit {
            let req_id = ctx.tool_call_request_id.clone().ok_or_else(|| {
                "Elicitation requires an active tool call context with a request ID".to_string()
            })?;

            let prompt = args
                .get("elicit_message")
                .and_then(|v| v.as_str())
                .unwrap_or("Enter input for process");

            let schema = serde_json::json!({
                "type": "object",
                "properties": {
                    "input": {
                        "type": "string",
                        "writeOnly": true,
                        "format": "password",
                        "description": prompt
                    }
                },
                "required": ["input"]
            });

            let outcome = self
                .context
                .session_manager
                .action_required()
                .request_and_wait(
                    ctx.session_id.clone(),
                    req_id,
                    prompt.to_string(),
                    schema,
                    Duration::from_secs(300),
                )
                .await
                .map_err(|e| format!("Elicitation failed: {}", e))?;

            let input_text = match outcome {
                crate::action_required_manager::ElicitationOutcome::Accept(val) => val
                    .get("input")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing 'input' field in elicitation response".to_string())?
                    .to_string(),
                crate::action_required_manager::ElicitationOutcome::Decline => {
                    return Err("User declined elicitation request".to_string());
                }
                crate::action_required_manager::ElicitationOutcome::Cancel => {
                    return Err("User cancelled elicitation request".to_string());
                }
            };

            let no_enter = args
                .get("no_enter")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let mut processes = self.processes.lock().await;
            let proc = processes
                .get_mut(task_id)
                .ok_or_else(|| format!("Process task '{}' not found", task_id))?;

            write_input_to_process(proc, input_text.as_bytes(), no_enter, false).await?;

            return Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "Successfully elicited input and sent it to process stdin for task {}",
                task_id
            ))]));
        }

        // Check if this is input for a task in InputRequired state (subagent response)
        if let Some(input) = args.get("input").and_then(|v| v.as_str()) {
            // First check if this is a task waiting for input
            {
                let registry = self.context.task_registry.lock().await;
                if let Some(task) = registry.get(task_id) {
                    if task.state == TaskState::InputRequired {
                        drop(registry);
                        let mut reg = self.context.task_registry.lock().await;
                        reg.resume(task_id);
                        return Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                            "Input sent to task {} (resumed from InputRequired state).",
                            task_id
                        ))]));
                    }
                }
            }

            // Otherwise send to process stdin
            return self.send_text_to_process(task_id, input, &args).await;
        }

        Err("Provide one of: 'input' (text), 'bytes' (raw), 'signal' (e.g. SIGINT), or set 'elicit: true'".to_string())
    }

    async fn send_text_to_process(
        &self,
        task_id: &str,
        input: &str,
        args: &JsonObject,
    ) -> Result<CallToolResult, String> {
        let no_enter = args
            .get("no_enter")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let close_stdin = args
            .get("close_stdin")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut processes = self.processes.lock().await;
        let proc = processes
            .get_mut(task_id)
            .ok_or_else(|| format!("Process task '{}' not found", task_id))?;

        write_input_to_process(proc, input.as_bytes(), no_enter, close_stdin).await?;

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Sent to {}: {}",
            task_id, input
        ))]))
    }

    async fn send_bytes_to_task(
        &self,
        task_id: &str,
        bytes: &[serde_json::Value],
    ) -> Result<CallToolResult, String> {
        let raw: Vec<u8> = bytes
            .iter()
            .filter_map(|v| v.as_u64().map(|b| b as u8))
            .collect();

        let mut processes = self.processes.lock().await;
        let proc = processes
            .get_mut(task_id)
            .ok_or_else(|| format!("Process task '{}' not found", task_id))?;

        write_input_to_process(proc, &raw, true, false).await?;

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Sent {} bytes to {}",
            raw.len(),
            task_id
        ))]))
    }

    async fn send_signal_to_task(
        &self,
        task_id: &str,
        signal: &str,
    ) -> Result<CallToolResult, String> {
        #[cfg(unix)]
        {
            use nix::sys::signal::{self as nix_signal, Signal};
            use nix::unistd::Pid;

            let sig = match signal.to_uppercase().as_str() {
                "SIGINT" => Signal::SIGINT,
                "SIGTERM" => Signal::SIGTERM,
                "SIGKILL" => Signal::SIGKILL,
                "SIGSTOP" => Signal::SIGSTOP,
                "SIGCONT" => Signal::SIGCONT,
                "SIGHUP" => Signal::SIGHUP,
                "SIGQUIT" => Signal::SIGQUIT,
                _ => return Err(format!(
                    "Unsupported signal: {}. Use SIGINT, SIGTERM, SIGKILL, SIGSTOP, SIGCONT, SIGHUP, or SIGQUIT.",
                    signal
                )),
            };

            let pid = {
                let processes = self.processes.lock().await;
                let proc = processes
                    .get(task_id)
                    .ok_or_else(|| format!("Process task '{}' not found", task_id))?;
                proc.child_id()
                    .ok_or_else(|| "Process already exited".to_string())? as i32
            };

            nix_signal::kill(Pid::from_raw(pid), sig)
                .map_err(|e| format!("Failed to send signal: {}", e))?;

            Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "Signal {} sent to {}",
                signal, task_id
            ))]))
        }

        #[cfg(not(unix))]
        {
            let _ = task_id;
            let _ = signal;
            Err("Signal sending is only supported on Unix".to_string())
        }
    }

    fn format_output(
        stdout: &str,
        stderr: &str,
        stdout_path: &str,
        stderr_path: Option<&str>,
    ) -> String {
        let mut out = String::new();
        out.push_str("── stdout ──\n");
        if stdout.is_empty() {
            out.push_str("(empty)\n");
        } else {
            out.push_str(stdout);
            if !stdout.ends_with('\n') {
                out.push('\n');
            }
        }
        out.push_str("\n── stderr ──\n");
        if stderr.is_empty() {
            out.push_str("(empty)\n");
        } else {
            out.push_str(stderr);
            if !stderr.ends_with('\n') {
                out.push('\n');
            }
        }
        out.push_str(&format!("\n── files ──\nstdout: {}\n", stdout_path));
        if let Some(path) = stderr_path {
            out.push_str(&format!("stderr: {}\n", path));
        }
        out
    }

    // ── read_output ──────────────────────────────────────────────────────

    pub async fn handle_read_output(
        &self,
        _ctx: &ToolCallContext,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, String> {
        let args = arguments.ok_or("Missing arguments")?;
        let task_id = args
            .get("task_id")
            .and_then(|v| v.as_str())
            .ok_or("Missing 'task_id' argument")?;
        let wait_for = args.get("wait_for").and_then(|v| v.as_str());
        let timeout_ms = args.get("timeout_ms").and_then(|v| v.as_u64());
        let strip_ansi = args
            .get("strip_ansi")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let limit_lines = args.get("limit_lines").and_then(|v| v.as_u64());

        // Update task activity
        if let Some(task) = self.context.task_registry.lock().await.get_mut(task_id) {
            task.last_activity = std::time::Instant::now();
        }

        let (stdout_path, stderr_path, mut stdout_pos, mut stderr_pos, is_running) = {
            let processes = self.processes.lock().await;
            let proc = processes
                .get(task_id)
                .ok_or_else(|| format!("Process task '{}' not found", task_id))?;
            let is_running = proc.child_id().is_some();
            (
                proc.stdout_path().to_string(),
                proc.stderr_path().map(|s| s.to_string()),
                match proc {
                    ManagedProcess::Pipe { stdout_pos, .. } => *stdout_pos,
                    ManagedProcess::Pty { stdout_pos, .. } => *stdout_pos,
                },
                match proc {
                    ManagedProcess::Pipe { stderr_pos, .. } => Some(*stderr_pos),
                    ManagedProcess::Pty { .. } => None,
                },
                is_running,
            )
        };

        if wait_for.is_some() || timeout_ms.is_some() {
            let max_wait = std::time::Duration::from_millis(timeout_ms.unwrap_or(30_000));
            let idle_timeout = std::time::Duration::from_millis(300);
            let deadline = tokio::time::Instant::now() + max_wait;
            let mut collected_stdout = String::new();
            let mut collected_stderr = String::new();
            let mut idle_since = tokio::time::Instant::now();

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                let (stdout_chunk, new_stdout_pos) =
                    read_from_position(&stdout_path, stdout_pos)
                        .map_err(|e| format!("Failed to read stdout log: {}", e))?;
                if !stdout_chunk.is_empty() {
                    collected_stdout.push_str(&stdout_chunk);
                    stdout_pos = new_stdout_pos;
                    idle_since = tokio::time::Instant::now();
                }

                let mut new_stderr_pos_val = stderr_pos;
                if let (Some(ref path), Some(pos)) = (&stderr_path, stderr_pos) {
                    let (stderr_chunk, new_pos) = read_from_position(path, pos)
                        .map_err(|e| format!("Failed to read stderr log: {}", e))?;
                    if !stderr_chunk.is_empty() {
                        collected_stderr.push_str(&stderr_chunk);
                        new_stderr_pos_val = Some(new_pos);
                        idle_since = tokio::time::Instant::now();
                    }
                }
                stderr_pos = new_stderr_pos_val;

                // Update offsets in process map
                {
                    let mut processes = self.processes.lock().await;
                    if let Some(proc) = processes.get_mut(task_id) {
                        match proc {
                            ManagedProcess::Pipe {
                                stdout_pos: sp,
                                stderr_pos: ep,
                                ..
                            } => {
                                *sp = stdout_pos;
                                if let Some(pos) = stderr_pos {
                                    *ep = pos;
                                }
                            }
                            ManagedProcess::Pty { stdout_pos: sp, .. } => {
                                *sp = stdout_pos;
                            }
                        }
                    }
                }

                // Check pattern
                if let Some(pattern) = wait_for {
                    if collected_stdout.contains(pattern) || collected_stderr.contains(pattern) {
                        let final_stdout = if strip_ansi {
                            String::from_utf8_lossy(&strip_ansi_escapes::strip(
                                collected_stdout.as_bytes(),
                            ))
                            .to_string()
                        } else {
                            collected_stdout
                        };
                        let final_stderr = if strip_ansi {
                            String::from_utf8_lossy(&strip_ansi_escapes::strip(
                                collected_stderr.as_bytes(),
                            ))
                            .to_string()
                        } else {
                            collected_stderr
                        };
                        return Ok(CallToolResult::success(vec![ContentBlock::text(
                            Self::format_output(
                                &final_stdout,
                                &final_stderr,
                                &stdout_path,
                                stderr_path.as_deref(),
                            ),
                        )]));
                    }
                }

                // Check if process exited
                if !is_running {
                    let final_stdout = if strip_ansi {
                        String::from_utf8_lossy(&strip_ansi_escapes::strip(
                            collected_stdout.as_bytes(),
                        ))
                        .to_string()
                    } else {
                        collected_stdout
                    };
                    let final_stderr = if strip_ansi {
                        String::from_utf8_lossy(&strip_ansi_escapes::strip(
                            collected_stderr.as_bytes(),
                        ))
                        .to_string()
                    } else {
                        collected_stderr
                    };
                    return Ok(CallToolResult::success(vec![ContentBlock::text(
                        Self::format_output(
                            &final_stdout,
                            &final_stderr,
                            &stdout_path,
                            stderr_path.as_deref(),
                        ),
                    )]));
                }

                // Timeout
                if tokio::time::Instant::now() >= deadline {
                    let final_stdout = if strip_ansi {
                        String::from_utf8_lossy(&strip_ansi_escapes::strip(
                            collected_stdout.as_bytes(),
                        ))
                        .to_string()
                    } else {
                        collected_stdout
                    };
                    let final_stderr = if strip_ansi {
                        String::from_utf8_lossy(&strip_ansi_escapes::strip(
                            collected_stderr.as_bytes(),
                        ))
                        .to_string()
                    } else {
                        collected_stderr
                    };
                    return Ok(CallToolResult::success(vec![ContentBlock::text(
                        Self::format_output(
                            &final_stdout,
                            &final_stderr,
                            &stdout_path,
                            stderr_path.as_deref(),
                        ),
                    )]));
                }

                // Idle timeout (only when no wait_for pattern)
                if wait_for.is_none()
                    && (!collected_stdout.is_empty() || !collected_stderr.is_empty())
                    && idle_since.elapsed() >= idle_timeout
                {
                    let final_stdout = if strip_ansi {
                        String::from_utf8_lossy(&strip_ansi_escapes::strip(
                            collected_stdout.as_bytes(),
                        ))
                        .to_string()
                    } else {
                        collected_stdout
                    };
                    let final_stderr = if strip_ansi {
                        String::from_utf8_lossy(&strip_ansi_escapes::strip(
                            collected_stderr.as_bytes(),
                        ))
                        .to_string()
                    } else {
                        collected_stderr
                    };
                    return Ok(CallToolResult::success(vec![ContentBlock::text(
                        Self::format_output(
                            &final_stdout,
                            &final_stderr,
                            &stdout_path,
                            stderr_path.as_deref(),
                        ),
                    )]));
                }
            }
        }

        let (stdout_str, stderr_str) = if let Some(limit) = limit_lines {
            let full_stdout = tokio::fs::read_to_string(&stdout_path)
                .await
                .unwrap_or_default();
            let final_stdout = tail_lines(&full_stdout, limit as usize);

            let final_stderr = if let Some(ref path) = stderr_path {
                let full_stderr = tokio::fs::read_to_string(path).await.unwrap_or_default();
                tail_lines(&full_stderr, limit as usize)
            } else {
                String::new()
            };

            (final_stdout, final_stderr)
        } else {
            let (stdout_chunk, new_stdout_pos) = read_from_position(&stdout_path, stdout_pos)
                .map_err(|e| format!("Failed to read stdout: {}", e))?;
            stdout_pos = new_stdout_pos;

            let stderr_chunk = if let (Some(ref path), Some(pos)) = (&stderr_path, stderr_pos) {
                let (stderr_chunk, new_pos) = read_from_position(path, pos)
                    .map_err(|e| format!("Failed to read stderr: {}", e))?;
                stderr_pos = Some(new_pos);
                stderr_chunk
            } else {
                String::new()
            };

            // Update offsets in process map
            {
                let mut processes = self.processes.lock().await;
                if let Some(proc) = processes.get_mut(task_id) {
                    match proc {
                        ManagedProcess::Pipe {
                            stdout_pos: sp,
                            stderr_pos: ep,
                            ..
                        } => {
                            *sp = stdout_pos;
                            if let Some(pos) = stderr_pos {
                                *ep = pos;
                            }
                        }
                        ManagedProcess::Pty { stdout_pos: sp, .. } => {
                            *sp = stdout_pos;
                        }
                    }
                }
            }

            (stdout_chunk, stderr_chunk)
        };

        let final_stdout = if strip_ansi {
            String::from_utf8_lossy(&strip_ansi_escapes::strip(stdout_str.as_bytes())).to_string()
        } else {
            stdout_str
        };
        let final_stderr = if strip_ansi {
            String::from_utf8_lossy(&strip_ansi_escapes::strip(stderr_str.as_bytes())).to_string()
        } else {
            stderr_str
        };

        Ok(CallToolResult::success(vec![ContentBlock::text(
            Self::format_output(
                &final_stdout,
                &final_stderr,
                &stdout_path,
                stderr_path.as_deref(),
            ),
        )]))
    }

    // ── cancel_task ─────────────────────────────────────────────────────

    pub async fn handle_cancel_task(
        &self,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, String> {
        let task_id = arguments
            .as_ref()
            .and_then(|a| a.get("task_id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Missing required parameter: task_id".to_string())?;

        // If this is a process task, kill the OS process and clean up files
        let mut processes = self.processes.lock().await;
        if let Some(proc) = processes.remove(task_id) {
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
        }
        drop(processes);

        let mut registry = self.context.task_registry.lock().await;
        if registry.get(task_id).is_none() {
            return Err(format!("Task '{}' not found", task_id));
        }
        registry.cancel(task_id);
        drop(registry);

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Task {} cancelled.",
            task_id
        ))]))
    }

    // ── list_tasks ──────────────────────────────────────────────────────

    pub async fn handle_list_tasks(&self) -> Result<CallToolResult, String> {
        let registry = self.context.task_registry.lock().await;
        let tasks = registry.list();
        if tasks.is_empty() {
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                "No active tasks.",
            )]));
        }

        let lines: Vec<String> = tasks
            .iter()
            .map(|j| {
                let source = match j.source {
                    TaskSource::McpTool => "tool",
                    TaskSource::Process => "process",
                    TaskSource::Timer => "timer",
                    TaskSource::Subagent => "subagent",
                    TaskSource::McpResource => "resource",
                    TaskSource::SwarmAgent => "swarm",
                };
                let state_str = match j.state {
                    TaskState::Working => "running",
                    TaskState::InputRequired => "waiting for input",
                    TaskState::Completed => "completed",
                    TaskState::Failed => "failed",
                    TaskState::Cancelled => "cancelled",
                };
                format!("  {} [{}] {} — {}", j.id, source, state_str, j.description)
            })
            .collect();

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Active tasks:\n{}",
            lines.join("\n")
        ))]))
    }

    // ── get_task ────────────────────────────────────────────────────────

    pub async fn handle_get_task(
        &self,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, String> {
        let task_id = arguments
            .as_ref()
            .and_then(|a| a.get("task_id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Missing required parameter: task_id".to_string())?;

        let mut registry = self.context.task_registry.lock().await;
        let task = registry
            .get(task_id)
            .ok_or_else(|| format!("Task '{}' not found", task_id))?;

        let state_str = match task.state {
            TaskState::Working => "running",
            TaskState::InputRequired => "waiting for input",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
        };

        let source = match task.source {
            TaskSource::McpTool => "tool",
            TaskSource::Process => "process",
            TaskSource::Timer => "timer",
            TaskSource::Subagent => "subagent",
            TaskSource::McpResource => "resource",
            TaskSource::SwarmAgent => "swarm",
        };

        let elapsed = task.created_at.elapsed();
        let elapsed_str = format_duration(elapsed);

        let result = if task.state.is_terminal() {
            let t = registry.remove(task_id);
            // Clean up process handle and delete log files when reaping
            let mut processes = self.processes.lock().await;
            if let Some(proc) = processes.remove(task_id) {
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
            }
            if let Some(task_val) = t {
                format!(
                    "Task {} [{}] {}\nSource: {}\nElapsed: {}\n{}",
                    task_val.id,
                    source,
                    state_str,
                    task_val.description,
                    elapsed_str,
                    task_val.result_summary.as_deref().unwrap_or("(no output)")
                )
            } else {
                format!("Task {} not found", task_id)
            }
        } else {
            format!(
                "Task {} [{}] {}\nSource: {}\nElapsed: {}",
                task.id, source, state_str, task.description, elapsed_str
            )
        };

        Ok(CallToolResult::success(vec![ContentBlock::text(result)]))
    }
}
