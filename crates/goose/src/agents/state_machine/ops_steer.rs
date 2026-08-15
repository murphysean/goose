//! Adds queued user guidance when the agent is between model and tool turns.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::agents::state_machine::operation::{
    applied, ends_turn, last_effective_role, messages_since_kickoff, not_applicable, Emitter,
    Operation, OperationResult,
};
use crate::conversation::message::Message;
use crate::conversation::{Conversation, EffectiveRole};
use crate::hooks::{HookContext, HookEvent, HookManager};
use crate::session::Session;

pub(crate) type SteerQueue = Arc<Mutex<VecDeque<Message>>>;

pub struct SteerOperation {
    queue: SteerQueue,
    hook_manager: HookManager,
    task_registry: crate::tasks::SharedTaskRegistry,
}

impl SteerOperation {
    pub(crate) fn new(
        queue: SteerQueue,
        hook_manager: HookManager,
        task_registry: crate::tasks::SharedTaskRegistry,
    ) -> Self {
        Self {
            queue,
            hook_manager,
            task_registry,
        }
    }
}

#[async_trait]
impl Operation for SteerOperation {
    fn name(&self) -> &'static str {
        "steer"
    }

    async fn run(
        &self,
        session: &Session,
        conversation: &Conversation,
        emit: &Emitter,
    ) -> Result<OperationResult> {
        let messages = messages_since_kickoff(conversation)?;
        let between_turns =
            ends_turn(messages) || last_effective_role(messages)? == EffectiveRole::Tool;
        if !between_turns {
            return not_applicable();
        }

        let pending: Vec<_> = self
            .queue
            .lock()
            .await
            .drain(..)
            .map(Message::with_steer)
            .collect();

        // Drain completed background tasks and include them alongside
        // any user steers so the LLM sees everything in one shot.
        let completed_tasks = {
            let reg = self.task_registry.lock().await;
            reg.finished()
                .iter()
                .map(|t| {
                    let summary = t.result_summary.clone().unwrap_or_default();
                    let id = t.id.clone();
                    let desc = t.description.clone();
                    (id, desc, summary)
                })
                .collect::<Vec<_>>()
        };
        if !completed_tasks.is_empty() {
            let mut reg = self.task_registry.lock().await;
            for (id, _, _) in &completed_tasks {
                reg.remove(id);
            }
            drop(reg);

            let mut batch = Vec::new();
            for (task_id, description, summary) in &completed_tasks {
                let msg = format!(
                    "System: Background task '{}' ({}) completed.\nResult: {}",
                    task_id, description, summary
                );
                batch.push(msg);
            }
            let steer_msg = Message::user()
                .with_text(batch.join("\n\n"))
                .with_visibility(false, true);
            emit.message(steer_msg).await;
        }

        if pending.is_empty() {
            return not_applicable();
        }

        let mut effects = Vec::with_capacity(pending.len());
        for message in pending {
            let context = HookContext::new(HookEvent::UserPromptSubmit, &session.id)
                .with_message(message.as_concat_text());
            self.hook_manager
                .emit(HookEvent::UserPromptSubmit, context)
                .await;
            let message = emit.message(message).await;
            effects.push(message.into());
        }
        applied(effects)
    }
}
