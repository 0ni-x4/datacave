use crate::messages::FrontendMessage;
use anyhow::Result;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn read_startup<S: AsyncRead + Unpin>(stream: &mut S) -> Result<FrontendMessage> {
    let len = stream.read_i32().await? as usize;
    let _protocol = stream.read_i32().await?;
    let mut buf = vec![0u8; len - 8];
    stream.read_exact(&mut buf).await?;
    let params = parse_params(&buf);
    Ok(FrontendMessage::Startup { params })
}

pub async fn read_message<S: AsyncRead + Unpin>(stream: &mut S) -> Result<FrontendMessage> {
    let msg_type = match stream.read_u8().await {
        Ok(v) => v,
        Err(_) => return Ok(FrontendMessage::Terminate),
    };
    let len = stream.read_i32().await? as usize;
    let mut buf = vec![0u8; len - 4];
    stream.read_exact(&mut buf).await?;
    match msg_type {
        b'Q' => {
            let sql = read_cstring(&buf);
            Ok(FrontendMessage::Query { sql })
        }
        b'p' => {
            let password = read_cstring(&buf);
            Ok(FrontendMessage::Password { password })
        }
        b'X' => Ok(FrontendMessage::Terminate),
        other => Ok(FrontendMessage::Unsupported { code: other }),
    }
}

fn parse_params(buf: &[u8]) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let mut i = 0;
    while i < buf.len() {
        let key = read_cstring(&buf[i..]);
        if key.is_empty() {
            break;
        }
        i += key.len() + 1;
        let value = read_cstring(&buf[i..]);
        i += value.len() + 1;
        params.insert(key, value);
    }
    params
}

fn read_cstring(buf: &[u8]) -> String {
    let mut end = 0;
    while end < buf.len() && buf[end] != 0 {
        end += 1;
    }
    String::from_utf8_lossy(&buf[..end]).to_string()
}
