//! Surfaces completed background tasks to the model between turns.

use anyhow::Result;
use async_trait::async_trait;

use crate::agents::state_machine::effects::GooseEffect;
use crate::agents::state_machine::{
    applied, ends_turn, last_effective_role, messages_since_kickoff, not_applicable, Emitter,
    Operation, OperationResult,
};
use crate::conversation::{Conversation, EffectiveRole};
use crate::session::Session;
use crate::tasks::{task_completion_message, SharedTaskRegistry};

pub struct TaskNotificationOperation {
    task_registry: SharedTaskRegistry,
}

impl TaskNotificationOperation {
    pub(crate) fn new(task_registry: SharedTaskRegistry) -> Self {
        Self { task_registry }
    }
}

#[async_trait]
impl Operation<Session, GooseEffect> for TaskNotificationOperation {
    fn name(&self) -> &'static str {
        "task_notification"
    }

    async fn run(
        &self,
        _session: &Session,
        conversation: &Conversation,
        emit: &Emitter,
    ) -> Result<OperationResult<GooseEffect>> {
        let messages = messages_since_kickoff(conversation)?;
        let between_turns =
            ends_turn(messages) || last_effective_role(messages)? == EffectiveRole::Tool;
        if !between_turns {
            return not_applicable();
        }

        let completed_tasks = {
            let reg = self.task_registry.lock().await;
            reg.finished()
                .iter()
                .map(|t| {
                    let summary = t.result_summary.clone().unwrap_or_default();
                    let id = t.id.clone();
                    let desc = t.description.clone();
                    let state = t.state.clone();
                    (id, desc, summary, state)
                })
                .collect::<Vec<_>>()
        };
        if completed_tasks.is_empty() {
            return not_applicable();
        }

        {
            let mut reg = self.task_registry.lock().await;
            for (id, _, _, _) in &completed_tasks {
                reg.remove(id);
            }
        }

        let mut effects = Vec::with_capacity(completed_tasks.len());
        for (task_id, description, summary, state) in &completed_tasks {
            let message = emit
                .message(task_completion_message(
                    task_id,
                    description,
                    state,
                    summary,
                ))
                .await;
            effects.push(message.into());
        }
        applied(effects)
    }
}
