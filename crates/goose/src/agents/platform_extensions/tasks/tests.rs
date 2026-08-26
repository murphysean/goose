use super::*;
use crate::agents::platform_extensions::PlatformExtensionContext;
use crate::agents::tool_execution::ToolCallContext;
use serde_json::json;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;

fn create_test_context() -> PlatformExtensionContext {
    let (task_registry, _) = crate::tasks::create_task_registry();
    PlatformExtensionContext {
        extension_manager: None,
        session_manager: Arc::new(crate::session::SessionManager::instance()),
        scheduler: None,
        session: None,
        use_login_shell_path: false,
        task_registry,
    }
}

fn parse_task_id(text: &str) -> String {
    let parts: Vec<&str> = text.split_whitespace().collect();
    parts[1].to_string()
}

#[tokio::test]
#[serial]
async fn test_tasks_lifecycle() {
    let ctx = create_test_context();
    let client = TasksClient::new(ctx.clone()).unwrap();

    // 1. Start a simple process task
    let args = json!({
        "kind": "process",
        "command": "echo 'Hello World'",
    });

    let tool_ctx =
        ToolCallContext::new("test-session".to_string(), None, Some("req-1".to_string()));

    let result = client
        .handle_start_task(args.as_object().cloned())
        .await
        .unwrap();
    let text = &result.content[0].as_text().unwrap().text;
    assert!(text.contains("started"));

    let task_id = parse_task_id(text);

    // Wait a bit for the command to finish and logs to be written.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. Read output from the task
    let read_args = json!({
        "task_id": task_id,
    });
    let read_result = client
        .handle_read_output(&tool_ctx, read_args.as_object().cloned())
        .await
        .unwrap();
    let read_text = &read_result.content[0].as_text().unwrap().text;

    // Verify output contains stdout section with our text
    assert!(
        read_text.contains("Hello World"),
        "stdout should contain 'Hello World', got: {}",
        read_text
    );
    assert!(
        read_text.contains("── stdout ──"),
        "output should have stdout section header"
    );
    assert!(
        read_text.contains("── files ──"),
        "output should have files section"
    );
    assert!(
        read_text.contains("stdout: /tmp/"),
        "output should reference stdout tmp file"
    );

    // 3. Reap the task (get_task)
    let get_args = json!({
        "task_id": task_id,
    });
    let get_result = client
        .handle_get_task(get_args.as_object().cloned())
        .await
        .unwrap();
    let get_text = &get_result.content[0].as_text().unwrap().text;
    assert!(get_text.contains(&task_id));

    // 4. Verify that files are cleaned up after get_task reaps
    // Extract the stdout file path from the output
    let stdout_file = read_text
        .lines()
        .find(|l| l.starts_with("stdout: "))
        .map(|l| l.trim_start_matches("stdout: ").trim())
        .unwrap();
    assert!(!std::path::Path::new(stdout_file).exists());
}

#[tokio::test]
#[serial]
async fn test_read_output_limit_lines() {
    let ctx = create_test_context();
    let client = TasksClient::new(ctx.clone()).unwrap();

    let args = json!({
        "kind": "process",
        "command": "printf 'line1\\nline2\\nline3\\n'",
    });

    let tool_ctx =
        ToolCallContext::new("test-session".to_string(), None, Some("req-2".to_string()));

    let result = client
        .handle_start_task(args.as_object().cloned())
        .await
        .unwrap();
    let text = &result.content[0].as_text().unwrap().text;
    let task_id = parse_task_id(text);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let read_args = json!({
        "task_id": task_id,
        "limit_lines": 2,
    });

    let read_result = client
        .handle_read_output(&tool_ctx, read_args.as_object().cloned())
        .await
        .unwrap();
    let read_text = &read_result.content[0].as_text().unwrap().text;

    // The stdout section should contain the last 2 lines (line2, line3)
    assert!(read_text.contains("line2"), "should contain line2");
    assert!(read_text.contains("line3"), "should contain line3");
    // line1 should not appear in the stdout section (it's before the last 2)
    // The stdout section is between "── stdout ──" and "── stderr ──"
    let stdout_section = read_text
        .split("── stderr ──")
        .next()
        .unwrap_or("")
        .split("── stdout ──")
        .nth(1)
        .unwrap_or("");
    assert!(
        !stdout_section.contains("line1"),
        "line1 should not be in stdout section"
    );

    // Clean up
    let get_args = json!({ "task_id": task_id });
    let _ = client.handle_get_task(get_args.as_object().cloned()).await;
}

#[tokio::test]
#[serial]
async fn test_stdin_interaction() {
    let ctx = create_test_context();
    let client = TasksClient::new(ctx.clone()).unwrap();

    // Start a process that waits for stdin and echoes it
    let args = json!({
        "kind": "process",
        "command": "read line; echo \"echoed: $line\"",
    });

    let tool_ctx =
        ToolCallContext::new("test-session".to_string(), None, Some("req-3".to_string()));

    let result = client
        .handle_start_task(args.as_object().cloned())
        .await
        .unwrap();
    let text = &result.content[0].as_text().unwrap().text;
    let task_id = parse_task_id(text);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send input
    let send_args = json!({
        "task_id": task_id,
        "input": "hello stdin",
    });

    client
        .handle_send_input(&tool_ctx, send_args.as_object().cloned())
        .await
        .unwrap();

    // Wait for it to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Read output
    let read_args = json!({
        "task_id": task_id,
    });
    let read_result = client
        .handle_read_output(&tool_ctx, read_args.as_object().cloned())
        .await
        .unwrap();
    let read_text = &read_result.content[0].as_text().unwrap().text;
    assert!(
        read_text.contains("echoed: hello stdin"),
        "stdout was: {}",
        read_text
    );

    // Clean up
    let get_args = json!({ "task_id": task_id });
    let _ = client.handle_get_task(get_args.as_object().cloned()).await;
}

#[tokio::test]
#[serial]
async fn test_wait_for_pattern_on_stderr() {
    let ctx = create_test_context();
    let client = TasksClient::new(ctx.clone()).unwrap();

    // Start a process that prints the pattern to stderr, not stdout
    let args = json!({
        "kind": "process",
        "command": "echo 'noise on stdout' && echo 'Server ready on port 8080' >&2",
        "wait_for": "ready on port",
    });

    let result = client
        .handle_start_task(args.as_object().cloned())
        .await
        .unwrap();
    let text = &result.content[0].as_text().unwrap().text;
    let task_id = parse_task_id(text);

    // Wait for the pattern to fire
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Check that the pattern was matched via the task registry
    let registry = ctx.task_registry.lock().await;
    let task = registry.get(&task_id).expect("task should exist");
    assert!(
        task.status_message
            .as_deref()
            .unwrap_or("")
            .contains("Pattern"),
        "pattern should have matched on stderr, got status: {:?}",
        task.status_message
    );
    drop(registry);

    // Clean up
    let get_args = json!({ "task_id": task_id });
    let _ = client.handle_get_task(get_args.as_object().cloned()).await;
}

#[tokio::test]
#[serial]
async fn test_wait_for_pattern_partial_line_no_newline() {
    let ctx = create_test_context();
    let client = TasksClient::new(ctx.clone()).unwrap();

    // Start a process that outputs a prompt without a trailing newline
    // printf doesn't add a newline, simulating a REPL prompt like ">>> "
    let args = json!({
        "kind": "process",
        "command": "printf 'Ready> '",
        "wait_for": "Ready>",
    });

    let result = client
        .handle_start_task(args.as_object().cloned())
        .await
        .unwrap();
    let text = &result.content[0].as_text().unwrap().text;
    let task_id = parse_task_id(text);

    // Wait for the pattern to fire
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Check that the pattern was matched despite no trailing newline
    let registry = ctx.task_registry.lock().await;
    let task = registry.get(&task_id).expect("task should exist");
    assert!(
        task.status_message
            .as_deref()
            .unwrap_or("")
            .contains("Pattern"),
        "pattern should have matched on partial line without newline, got status: {:?}",
        task.status_message
    );
    drop(registry);

    // Clean up
    let get_args = json!({ "task_id": task_id });
    let _ = client.handle_get_task(get_args.as_object().cloned()).await;
}

#[tokio::test]
#[serial]
async fn test_read_output_wait_for_on_stderr() {
    let ctx = create_test_context();
    let client = TasksClient::new(ctx.clone()).unwrap();

    // Start a process that outputs the target pattern only to stderr
    let args = json!({
        "kind": "process",
        "command": "sleep 0.1 && echo 'build complete' >&2",
    });

    let tool_ctx = ToolCallContext::new(
        "test-session".to_string(),
        None,
        Some("req-stderr".to_string()),
    );

    let result = client
        .handle_start_task(args.as_object().cloned())
        .await
        .unwrap();
    let text = &result.content[0].as_text().unwrap().text;
    let task_id = parse_task_id(text);

    // read_output with wait_for should find the pattern in stderr
    let read_args = json!({
        "task_id": task_id,
        "wait_for": "build complete",
        "timeout_ms": 5000,
    });
    let read_result = client
        .handle_read_output(&tool_ctx, read_args.as_object().cloned())
        .await
        .unwrap();
    let read_text = &read_result.content[0].as_text().unwrap().text;

    assert!(
        read_text.contains("build complete"),
        "read_output wait_for should find pattern in stderr, got: {}",
        read_text
    );

    // Clean up
    let get_args = json!({ "task_id": task_id });
    let _ = client.handle_get_task(get_args.as_object().cloned()).await;
}
