use goose::agents::Agent;
use goose::agents::AgentConfig;
use goose::agents::GoosePlatform;
use goose::config::Config;
use goose::config::GooseMode;
use goose::session::SessionManager;
use std::sync::Arc;

#[tokio::test]
async fn test_compilation_of_promotion_logic() {
    let config = Config::global();
    let agent_config = AgentConfig::new(
        Arc::new(SessionManager::instance()),
        goose::config::permission::PermissionManager::instance(),
        None,
        config.get_goose_mode().unwrap_or(GooseMode::Auto),
        config.get_goose_disable_session_naming().unwrap_or(false),
        GoosePlatform::GooseCli,
    );
    let _agent = Agent::with_config(agent_config);

    let limit: u64 = config
        .get_param("GOOSE_TOOL_EXECUTION_LIMIT_MS")
        .unwrap_or(2000);
    assert_eq!(limit, 2000);
}
