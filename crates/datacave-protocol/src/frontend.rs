use crate::messages::{CloseTarget, DescribeTarget, FrontendMessage};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt};

fn advance_cstring(buf: &mut &[u8]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let s = String::from_utf8_lossy(&buf[..end]).to_string();
    *buf = &buf[(end + 1).min(buf.len())..];
    s
}

fn advance_i16(buf: &mut &[u8]) -> Result<i16> {
    if buf.len() < 2 {
        return Err(anyhow!("buffer too short for i16"));
    }
    let v = i16::from_be_bytes([buf[0], buf[1]]);
    *buf = &buf[2..];
    Ok(v)
}

fn advance_i32(buf: &mut &[u8]) -> Result<i32> {
    if buf.len() < 4 {
        return Err(anyhow!("buffer too short for i32"));
    }
    let v = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];
    Ok(v)
}

fn parse_parse(buf: &[u8]) -> Result<FrontendMessage> {
    let mut cur = buf;
    let statement_name = advance_cstring(&mut cur);
    let query = advance_cstring(&mut cur);
    let n = advance_i16(&mut cur)? as usize;
    let mut param_oids = Vec::with_capacity(n);
    for _ in 0..n {
        param_oids.push(advance_i32(&mut cur)?);
    }
    Ok(FrontendMessage::Parse {
        statement_name,
        query,
        param_oids,
    })
}

fn parse_bind(buf: &[u8]) -> Result<FrontendMessage> {
    let mut cur = buf;
    let portal_name = advance_cstring(&mut cur);
    let statement_name = advance_cstring(&mut cur);
    let n_fmt = advance_i16(&mut cur)? as usize;
    let mut param_format_codes = Vec::with_capacity(n_fmt);
    for _ in 0..n_fmt {
        param_format_codes.push(advance_i16(&mut cur)?);
    }
    let n_params = advance_i16(&mut cur)? as usize;
    let mut param_values = Vec::with_capacity(n_params);
    for _ in 0..n_params {
        let len = advance_i32(&mut cur)?;
        if len < 0 {
            param_values.push(None);
        } else {
            let len = len as usize;
            if cur.len() < len {
                return Err(anyhow!("bind buffer too short for param value"));
            }
            param_values.push(Some(cur[..len].to_vec()));
            let (_, rest) = cur.split_at(len);
            cur = rest;
        }
    }
    let n_res = advance_i16(&mut cur)? as usize;
    let mut result_format_codes = Vec::with_capacity(n_res);
    for _ in 0..n_res {
        result_format_codes.push(advance_i16(&mut cur)?);
    }
    Ok(FrontendMessage::Bind {
        portal_name,
        statement_name,
        param_format_codes,
        param_values,
        result_format_codes,
    })
}

fn parse_describe(buf: &[u8]) -> Result<FrontendMessage> {
    let mut cur = buf;
    if cur.is_empty() {
        return Err(anyhow!("describe buffer empty"));
    }
    let target = match cur[0] {
        b'S' => DescribeTarget::Statement,
        b'P' => DescribeTarget::Portal,
        other => return Err(anyhow!("invalid describe target: {}", other)),
    };
    let (_, rest) = cur.split_at(1);
    cur = rest;
    let name = advance_cstring(&mut cur);
    Ok(FrontendMessage::Describe { target, name })
}

fn parse_execute(buf: &[u8]) -> Result<FrontendMessage> {
    let mut cur = buf;
    let portal_name = advance_cstring(&mut cur);
    let max_rows = advance_i32(&mut cur)?;
    Ok(FrontendMessage::Execute {
        portal_name,
        max_rows,
    })
}

fn parse_close(buf: &[u8]) -> Result<FrontendMessage> {
    let mut cur = buf;
    if cur.is_empty() {
        return Err(anyhow!("close buffer empty"));
    }
    let target = match cur[0] {
        b'S' => CloseTarget::Statement,
        b'P' => CloseTarget::Portal,
        other => return Err(anyhow!("invalid close target: {}", other)),
    };
    let (_, rest) = cur.split_at(1);
    cur = rest;
    let name = advance_cstring(&mut cur);
    Ok(FrontendMessage::Close { target, name })
}

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
        b'P' => parse_parse(&buf),
        b'B' => parse_bind(&buf),
        b'D' => parse_describe(&buf),
        b'E' => parse_execute(&buf),
        b'S' => Ok(FrontendMessage::Sync),
        b'H' => Ok(FrontendMessage::Flush),
        b'C' => parse_close(&buf),
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
