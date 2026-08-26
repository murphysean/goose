use std::sync::Arc;
use tokio::process::Child;
use tokio::sync::Mutex;

pub enum ManagedProcess {
    Pipe {
        child: Child,
        stdin: Option<tokio::process::ChildStdin>,
        stdout_path: String,
        stderr_path: String,
        stdout_pos: u64,
        stderr_pos: u64,
    },
    Pty {
        child: tokio::process::Child,
        pty_writer: Arc<Mutex<pty_process::OwnedWritePty>>,
        stdout_path: String,
        stdout_pos: u64,
    },
}

impl ManagedProcess {
    pub fn child_id(&self) -> Option<u32> {
        match self {
            ManagedProcess::Pipe { child, .. } => child.id(),
            ManagedProcess::Pty { child, .. } => child.id(),
        }
    }

    pub fn stdout_path(&self) -> &str {
        match self {
            ManagedProcess::Pipe { stdout_path, .. } => stdout_path,
            ManagedProcess::Pty { stdout_path, .. } => stdout_path,
        }
    }

    pub fn stderr_path(&self) -> Option<&str> {
        match self {
            ManagedProcess::Pipe { stderr_path, .. } => Some(stderr_path),
            ManagedProcess::Pty { .. } => None,
        }
    }
}

pub fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty duration string".to_string());
    }

    let mut total_secs = 0u64;
    let mut current = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current.push(ch);
        } else {
            let value: u64 = current
                .parse()
                .map_err(|_| format!("Invalid duration: {}", s))?;
            match ch {
                's' => total_secs += value,
                'm' => total_secs += value * 60,
                'h' => total_secs += value * 3600,
                _ => return Err(format!("Unknown duration unit '{}' in: {}", ch, s)),
            }
            current.clear();
        }
    }

    if !current.is_empty() {
        total_secs += current
            .parse::<u64>()
            .map_err(|_| format!("Invalid duration: {}", s))?;
    }

    Ok(std::time::Duration::from_secs(total_secs))
}

pub fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

pub fn read_from_position(path: &str, pos: u64) -> std::io::Result<(String, u64)> {
    use std::io::{Read, Seek};
    let mut file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len();
    if pos >= file_size {
        return Ok((String::new(), pos));
    }
    file.seek(std::io::SeekFrom::Start(pos))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    Ok((String::from_utf8_lossy(&data).to_string(), file_size))
}

pub fn tail_lines(content: &str, count: usize) -> String {
    let lines: Vec<&str> = content.lines().collect();
    if lines.len() > count {
        lines[lines.len() - count..].join("\n")
    } else {
        content.to_string()
    }
}

pub async fn write_input_to_process(
    proc: &mut ManagedProcess,
    input_bytes: &[u8],
    no_enter: bool,
    close_stdin: bool,
) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    match proc {
        ManagedProcess::Pipe {
            stdin: opt_stdin, ..
        } => {
            let stdin = opt_stdin
                .as_mut()
                .ok_or("Process stdin not available (already closed)")?;

            stdin
                .write_all(input_bytes)
                .await
                .map_err(|e| format!("Failed to write to stdin: {}", e))?;
            stdin
                .flush()
                .await
                .map_err(|e| format!("Failed to flush stdin: {}", e))?;

            if !no_enter {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                stdin
                    .write_all(b"\n")
                    .await
                    .map_err(|e| format!("Failed to write newline: {}", e))?;
                stdin
                    .flush()
                    .await
                    .map_err(|e| format!("Failed to flush stdin: {}", e))?;
            }

            if close_stdin {
                *opt_stdin = None;
            }
        }
        ManagedProcess::Pty { pty_writer, .. } => {
            let mut w = pty_writer.lock().await;
            w.write_all(input_bytes)
                .await
                .map_err(|e| format!("Failed to write to PTY: {}", e))?;
            w.flush()
                .await
                .map_err(|e| format!("Failed to flush PTY: {}", e))?;

            if !no_enter {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                w.write_all(b"\r\n")
                    .await
                    .map_err(|e| format!("Failed to write PTY newline: {}", e))?;
                w.flush()
                    .await
                    .map_err(|e| format!("Failed to flush PTY: {}", e))?;
            }
        }
    }
    Ok(())
}
