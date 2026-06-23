use goose_providers::conversation::message::Message;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// An item that can be queued for the next agent turn.
#[derive(Debug, Clone)]
pub enum TurnInput {
    /// User-authored message text.
    User(String),
    /// System-generated prompt (e.g. task completion, notification interrupt).
    System(String),
}

/// A queue that accumulates user messages and system events while the agent is
/// busy, then flushes them into a single combined user message for the next turn.
///
/// Both CLI and server use this to allow input to arrive at any time without
/// blocking on the agent's turn cycle.
#[derive(Clone)]
pub struct TurnQueue {
    inner: Arc<Mutex<Vec<TurnInput>>>,
    notify: Arc<Notify>,
}

impl TurnQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Push a user message onto the queue.
    pub async fn push_user(&self, text: String) {
        self.inner.lock().await.push(TurnInput::User(text));
        self.notify.notify_one();
    }

    /// Push a system event (task completion, interrupt, etc.) onto the queue.
    pub async fn push_system(&self, text: String) {
        self.inner.lock().await.push(TurnInput::System(text));
        self.notify.notify_one();
    }

    /// Wait until at least one item is available, then drain and combine all
    /// queued items into a single user Message. Returns None only if the queue
    /// is permanently closed (it never is in normal operation).
    ///
    /// Multiple user messages become separate paragraphs. System events are
    /// included inline, preserving arrival order.
    pub async fn flush(&self) -> Message {
        // Wait for at least one item
        loop {
            {
                let queue = self.inner.lock().await;
                if !queue.is_empty() {
                    break;
                }
            }
            self.notify.notified().await;
        }

        let items = {
            let mut queue = self.inner.lock().await;
            std::mem::take(&mut *queue)
        };

        let combined = combine_inputs(&items);
        Message::user().with_text(&combined)
    }

    /// Non-blocking drain: returns a combined message if items are queued,
    /// or None if the queue is empty.
    pub async fn try_flush(&self) -> Option<Message> {
        let items = {
            let mut queue = self.inner.lock().await;
            if queue.is_empty() {
                return None;
            }
            std::mem::take(&mut *queue)
        };

        let combined = combine_inputs(&items);
        Some(Message::user().with_text(&combined))
    }

    /// Check if there are pending items without consuming them.
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }
}

impl Default for TurnQueue {
    fn default() -> Self {
        Self::new()
    }
}

fn combine_inputs(items: &[TurnInput]) -> String {
    items
        .iter()
        .map(|item| match item {
            TurnInput::User(text) => text.clone(),
            TurnInput::System(text) => text.clone(),
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}
