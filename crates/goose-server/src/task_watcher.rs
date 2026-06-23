use goose::tasks::{TaskEvent, TaskSource, TaskState};
use goose::turn_queue::TurnQueue;
use tokio::sync::mpsc::UnboundedReceiver;

/// Spawn a background task that listens for task events and pushes them
/// onto the session's turn queue. The turn driver (in the reply path)
/// will flush and deliver them to the agent on the next turn boundary.
pub fn spawn_task_watcher(
    session_id: String,
    mut task_event_rx: UnboundedReceiver<TaskEvent>,
    turn_queue: TurnQueue,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = task_event_rx.recv().await {
            let prompt = synthesize_prompt(&event);
            tracing::info!(
                session_id = %session_id,
                "Task event received, queuing for next turn"
            );
            turn_queue.push_system(prompt).await;
        }

        tracing::debug!("Task watcher exiting — event channel closed");
    })
}

fn synthesize_prompt(event: &TaskEvent) -> String {
    match event {
        TaskEvent::BatchCompleted {
            batch_id,
            task_summaries,
        } => {
            let summary_text: Vec<String> = task_summaries
                .iter()
                .map(|s| {
                    let status = match s.state {
                        TaskState::Completed => "✓ completed",
                        TaskState::Failed => "✗ failed",
                        _ => "unknown",
                    };
                    format!(
                        "  • {} \"{}\": {} ({:.0?})",
                        s.id, s.description, status, s.duration
                    )
                })
                .collect();

            format!(
                "System: All background tasks in batch '{}' have completed:\n{}\n\n\
                 Use load(source: \"<id>\") to examine results and determine next steps.",
                batch_id,
                summary_text.join("\n")
            )
        }
        TaskEvent::TaskCompleted { task } => {
            let status = match task.state {
                TaskState::Completed => "✓ completed",
                TaskState::Failed => "✗ failed",
                _ => "finished",
            };
            let follow_up = match task.source {
                TaskSource::Process => {
                    format!(
                        "Use tasks__read_output(process_id: \"{}\") to see the output.",
                        task.id
                    )
                }
                TaskSource::Timer => {
                    format!("The reminder message was: \"{}\"", task.description)
                }
                _ => {
                    format!(
                        "Use get_task(task_id: \"{}\") or load(source: \"{}\") to examine the result.",
                        task.id, task.id
                    )
                }
            };
            format!(
                "System: Background task {} \"{}\" has {} ({:.0?}).\n{}",
                task.id, task.description, status, task.duration, follow_up
            )
        }
        TaskEvent::TaskNeedsInput { task_id, question } => {
            format!(
                "System: Background task '{}' needs your input: {}",
                task_id, question
            )
        }
        TaskEvent::PatternMatched {
            task_id,
            pattern,
            context,
        } => {
            format!(
                "System: Background task '{}' matched pattern \"{}\": {}",
                task_id, pattern, context
            )
        }
    }
}
