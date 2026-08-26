//! Unified Task Registry for tracking all async work in goose.
//!
//! Every background operation (subagent, process, MCP resource subscription,
//! long-running tool call) is represented as a Task with consistent lifecycle,
//! notification, and tracking semantics.
//!
//! ## State Machine
//!
//! ```text
//! Working → Complete
//! Working → Failed
//! Working → Cancelled
//! Working → InputRequired → Working (after input provided)
//! Working → (event: PatternMatched) → still Working
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use crate::conversation::message::{Message, MessageContentBlock, SystemNotificationType};

/// Unique identifier for a task.
pub type TaskId = String;

/// Unique identifier for a batch of related tasks.
pub type BatchId = String;

/// The source/type of a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSource {
    /// A subagent spawned via delegate(async: true)
    Subagent,
    /// A long-running process (shell command, build, etc.)
    Process,
    /// An MCP resource subscription being watched
    McpResource,
    /// A long-running MCP tool call
    McpTool,
    /// An orchestrator-managed swarm agent
    SwarmAgent,
    /// A scheduled timer/reminder
    Timer,
}

/// Current state of a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    /// Actively running / in progress.
    Working,
    /// The task needs input from the user or parent agent to continue.
    InputRequired,
    /// Successfully completed.
    Completed,
    /// Errored or crashed.
    Failed,
    /// Explicitly cancelled by user or agent.
    Cancelled,
}

impl TaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Which state transitions should trigger an interrupt (forced turn).
#[derive(Debug, Clone)]
pub struct InterruptPolicy {
    pub on_complete: bool,
    pub on_failed: bool,
    pub on_input_required: bool,
    pub on_pattern_matched: bool,
}

impl Default for InterruptPolicy {
    fn default() -> Self {
        Self {
            on_complete: true,
            on_failed: true,
            on_input_required: true,
            on_pattern_matched: true,
        }
    }
}

/// Metadata hints for task polling/retention behavior.
#[derive(Debug, Clone)]
pub struct TaskMeta {
    /// Don't poll more often than this.
    pub min_poll_interval_ms: u64,
    /// After reaching a terminal state, data is retained for this long.
    pub ttl_after_completion_s: u64,
    /// Which transitions trigger a forced turn.
    pub interrupt_on: InterruptPolicy,
    /// For timer tasks: when the timer fires.
    pub deadline: Option<Instant>,
}

impl Default for TaskMeta {
    fn default() -> Self {
        Self {
            min_poll_interval_ms: 1000,
            ttl_after_completion_s: 300,
            interrupt_on: InterruptPolicy::default(),
            deadline: None,
        }
    }
}

/// Policy for when a task's completion should trigger an assistant turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum NotifyPolicy {
    /// Only surface in MOIM. Never force a turn on its own.
    Informational,
    /// Force assistant turn when THIS specific task completes/fails.
    OnCompletion,
    /// Force assistant turn only when ALL tasks in the same batch are done.
    #[default]
    OnBatchCompletion,
}

/// A timestamped notification from a task.
#[derive(Debug, Clone)]
pub struct TaskNotification {
    pub timestamp: Instant,
    pub message: String,
}

/// A single tracked task.
#[derive(Debug)]
pub struct Task {
    pub id: TaskId,
    pub source: TaskSource,
    pub description: String,
    pub state: TaskState,
    pub batch_id: Option<BatchId>,
    pub notify_policy: NotifyPolicy,
    pub meta: TaskMeta,
    pub created_at: Instant,
    pub last_activity: Instant,
    /// Accumulated wall-clock time spent in Working state.
    /// Updated when transitioning out of Working or into InputRequired.
    pub worked_duration: Duration,
    pub notifications: Vec<TaskNotification>,
    /// Human-readable message describing the current task state (MCP Tasks `statusMessage`).
    pub status_message: Option<String>,
    /// Structured result for completed tasks (MCP Tasks `result`).
    pub result: Option<serde_json::Value>,
    /// Structured error for failed tasks (MCP Tasks `error`).
    pub error: Option<serde_json::Value>,
    /// Flat string summary (legacy, kept for backward compatibility).
    pub result_summary: Option<String>,
    /// When the task is in InputRequired state, this holds the question
    /// or prompt that the task is waiting on. get_task returns this
    /// without reaping the task.
    pub input_request: Option<String>,
    /// Time-to-live in milliseconds from creation (MCP Tasks `ttlMs`).
    /// The task may be discarded after this elapses. None = unlimited.
    pub ttl_ms: Option<u64>,
    /// Suggested polling interval in milliseconds (MCP Tasks `pollIntervalMs`).
    /// Clients SHOULD honor this to avoid overwhelming the server.
    pub poll_interval_ms: Option<u64>,
    /// Token to cancel the underlying operation. Firing it sends
    /// MCP `notifications/cancelled` to the server, which should abort
    /// the tool call and resolve the future.
    pub cancellation_token: Option<CancellationToken>,
}

/// Event emitted when an actionable condition is met.
#[derive(Debug, Clone)]
pub enum TaskEvent {
    /// A batch of tasks has fully completed.
    BatchCompleted {
        batch_id: BatchId,
        task_summaries: Vec<TaskSummary>,
    },
    /// A single task completed with OnCompletion policy.
    TaskCompleted { task: TaskSummary },
    /// A task needs input to continue.
    TaskNeedsInput { task_id: TaskId, question: String },
    /// A pattern was matched in the task's output stream (task still working).
    PatternMatched {
        task_id: TaskId,
        pattern: String,
        context: String,
    },
}

/// Summary of a completed task (for event payloads).
#[derive(Debug, Clone)]
pub struct TaskSummary {
    pub id: TaskId,
    pub source: TaskSource,
    pub description: String,
    pub state: TaskState,
    pub duration: Duration,
    pub result_summary: Option<String>,
    pub status_message: Option<String>,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

/// The Task Registry — tracks all async work for a session.
pub struct TaskRegistry {
    tasks: HashMap<TaskId, Task>,
    event_tx: mpsc::UnboundedSender<TaskEvent>,
}

impl TaskRegistry {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<TaskEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        (
            Self {
                tasks: HashMap::new(),
                event_tx,
            },
            event_rx,
        )
    }

    pub fn register(&mut self, task: Task) {
        self.tasks.insert(task.id.clone(), task);
    }

    pub fn get(&self, id: &str) -> Option<&Task> {
        self.tasks.get(id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut Task> {
        self.tasks.get_mut(id)
    }

    /// Mark a task as completed.
    pub fn complete(&mut self, id: &str, summary: Option<String>) {
        self.transition(id, TaskState::Completed, summary);
    }

    /// Mark a task as completed with a structured result (MCP Tasks `result`).
    pub fn complete_with_result(
        &mut self,
        id: &str,
        result: serde_json::Value,
        status_message: Option<String>,
    ) {
        if let Some(task) = self.tasks.get_mut(id) {
            task.result = Some(result);
            task.status_message = status_message;
        }
        self.transition(id, TaskState::Completed, None);
    }

    /// Mark a task as failed.
    pub fn fail(&mut self, id: &str, summary: Option<String>) {
        self.transition(id, TaskState::Failed, summary);
    }

    /// Mark a task as failed with a structured error (MCP Tasks `error`).
    pub fn fail_with_error(
        &mut self,
        id: &str,
        error: serde_json::Value,
        status_message: Option<String>,
    ) {
        if let Some(task) = self.tasks.get_mut(id) {
            task.error = Some(error);
            task.status_message = status_message;
        }
        self.transition(id, TaskState::Failed, None);
    }

    /// Mark a task as cancelled. Fires the cancellation token if present,
    /// which sends MCP `notifications/cancelled` to abort the operation.
    pub fn cancel(&mut self, id: &str) {
        if let Some(task) = self.tasks.get(id) {
            if let Some(token) = &task.cancellation_token {
                token.cancel();
            }
        }
        self.transition(id, TaskState::Cancelled, Some("cancelled".into()));
    }

    /// Transition a task to InputRequired and emit event.
    /// Stores the question on the task so read_task can surface it.
    pub fn needs_input(&mut self, id: &str, question: String) {
        if let Some(task) = self.tasks.get_mut(id) {
            if task.state == TaskState::Working {
                task.worked_duration += task.last_activity.elapsed();
            }
            task.state = TaskState::InputRequired;
            task.input_request = Some(question.clone());
            task.last_activity = Instant::now();
            if task.meta.interrupt_on.on_input_required {
                let _ = self.event_tx.send(TaskEvent::TaskNeedsInput {
                    task_id: id.to_string(),
                    question,
                });
            }
        }
    }

    /// Resume a task from InputRequired back to Working.
    pub fn resume(&mut self, id: &str) {
        if let Some(task) = self.tasks.get_mut(id) {
            if task.state == TaskState::InputRequired {
                task.state = TaskState::Working;
                task.last_activity = Instant::now();
            }
        }
    }

    /// Emit a pattern-matched event (task stays Working).
    pub fn pattern_matched(&mut self, id: &str, pattern: String, context: String) {
        if let Some(task) = self.tasks.get_mut(id) {
            task.last_activity = Instant::now();
            task.notifications.push(TaskNotification {
                timestamp: Instant::now(),
                message: format!("pattern matched: {}", pattern),
            });
            // Surface the match on the task itself so get_task/list_tasks can
            // report it even before the event channel is consumed.
            task.status_message = Some(format!("Pattern '{}' matched", pattern));
            if task.meta.interrupt_on.on_pattern_matched {
                let _ = self.event_tx.send(TaskEvent::PatternMatched {
                    task_id: id.to_string(),
                    pattern,
                    context,
                });
            }
        }
    }

    /// Add a notification to a task's log.
    pub fn notify(&mut self, id: &str, message: String) {
        if let Some(task) = self.tasks.get_mut(id) {
            task.last_activity = Instant::now();
            task.notifications.push(TaskNotification {
                timestamp: Instant::now(),
                message,
            });
        }
    }

    pub fn list(&self) -> Vec<&Task> {
        self.tasks.values().collect()
    }

    pub fn running(&self) -> Vec<&Task> {
        self.tasks
            .values()
            .filter(|t| t.state == TaskState::Working)
            .collect()
    }

    pub fn finished(&self) -> Vec<&Task> {
        self.tasks
            .values()
            .filter(|t| t.state.is_terminal())
            .collect()
    }

    pub fn remove(&mut self, id: &str) -> Option<Task> {
        self.tasks.remove(id)
    }

    /// Garbage collect tasks past their TTL.
    pub fn gc(&mut self) {
        let now = Instant::now();
        self.tasks.retain(|_, task| {
            if task.state.is_terminal() {
                let elapsed = now.duration_since(task.last_activity);
                elapsed.as_secs() < task.meta.ttl_after_completion_s
            } else {
                true
            }
        });
    }

    fn transition(&mut self, id: &str, new_state: TaskState, summary: Option<String>) {
        let Some(task) = self.tasks.get_mut(id) else {
            return;
        };
        if task.state.is_terminal() {
            return;
        }

        // Accumulate worked duration when transitioning out of Working
        if task.state == TaskState::Working {
            task.worked_duration += task.last_activity.elapsed();
        }

        task.state = new_state.clone();
        task.last_activity = Instant::now();
        task.result_summary = summary.clone();
        if let Some(msg) = summary {
            task.status_message = Some(msg);
        }

        let should_interrupt = match &new_state {
            TaskState::Completed => task.meta.interrupt_on.on_complete,
            TaskState::Failed => task.meta.interrupt_on.on_failed,
            _ => false,
        };

        match &task.notify_policy {
            NotifyPolicy::OnCompletion => {
                if should_interrupt {
                    let summary = make_task_summary(task);
                    let _ = self
                        .event_tx
                        .send(TaskEvent::TaskCompleted { task: summary });
                }
            }
            NotifyPolicy::OnBatchCompletion => {
                if let Some(batch_id) = task.batch_id.clone() {
                    self.check_batch_completion(&batch_id);
                }
            }
            NotifyPolicy::Informational => {}
        }
    }

    fn check_batch_completion(&self, batch_id: &str) {
        let batch_tasks: Vec<&Task> = self
            .tasks
            .values()
            .filter(|t| t.batch_id.as_deref() == Some(batch_id))
            .collect();

        if batch_tasks.is_empty() {
            return;
        }

        let all_done = batch_tasks.iter().all(|t| t.state.is_terminal());

        if all_done {
            let summaries: Vec<TaskSummary> =
                batch_tasks.iter().copied().map(make_task_summary).collect();
            let _ = self.event_tx.send(TaskEvent::BatchCompleted {
                batch_id: batch_id.to_string(),
                task_summaries: summaries,
            });
        }
    }
}

/// Build a summary snapshot from a task, measuring worked duration.
/// Exists as a free function so it can be called inside a mutable borrow of TaskRegistry.
pub fn make_task_summary(task: &Task) -> TaskSummary {
    let duration = if task.state == TaskState::Working {
        // Still working - account for time since last activity
        task.worked_duration + task.last_activity.elapsed()
    } else {
        task.worked_duration
    };
    TaskSummary {
        id: task.id.clone(),
        source: task.source.clone(),
        description: task.description.clone(),
        state: task.state.clone(),
        duration,
        result_summary: task.result_summary.clone(),
        status_message: task.status_message.clone(),
        result: task.result.clone(),
        error: task.error.clone(),
    }
}

/// Thread-safe handle to the task registry.
pub type SharedTaskRegistry = Arc<Mutex<TaskRegistry>>;

/// Create a new shared task registry and its event receiver.
pub fn create_task_registry() -> (SharedTaskRegistry, mpsc::UnboundedReceiver<TaskEvent>) {
    let (registry, event_rx) = TaskRegistry::new();
    (Arc::new(Mutex::new(registry)), event_rx)
}

/// Human-readable terminal state label for a task.
pub fn task_state_label(state: &TaskState) -> &'static str {
    match state {
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
        _ => "finished",
    }
}

/// Model-visible text describing a completed background task. This is what the
/// agent actually reads so it can react to the completion.
pub fn task_completion_text(
    task_id: &str,
    description: &str,
    state: &TaskState,
    summary: &str,
) -> String {
    format!(
        "System: Background task '{}' ({}) {}.\nResult: {}",
        task_id,
        description,
        task_state_label(state),
        summary
    )
}

/// Build a user message that surfaces a completed background task to both the
/// model and the UI. The `Text` block carries the model-visible notification
/// (providers strip `SystemNotification` blocks), while the `SystemNotification`
/// block lets the client render a distinct notification bubble.
pub fn task_completion_message(
    task_id: &str,
    description: &str,
    state: &TaskState,
    summary: &str,
) -> Message {
    let text = task_completion_text(task_id, description, state, summary);
    Message::user()
        .with_text(text.clone())
        .with_content(MessageContentBlock::system_notification(
            SystemNotificationType::InlineMessage,
            text,
        ))
}
