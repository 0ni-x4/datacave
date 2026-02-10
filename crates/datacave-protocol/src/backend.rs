use crate::messages::BackendMessage;
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub async fn write_message<S: AsyncWrite + Unpin>(stream: &mut S, msg: BackendMessage) -> Result<()> {
    let mut buf = BytesMut::new();
    match msg {
        BackendMessage::AuthenticationOk => {
            buf.put_u8(b'R');
            buf.put_i32(8);
            buf.put_i32(0);
        }
        BackendMessage::AuthenticationCleartextPassword => {
            buf.put_u8(b'R');
            buf.put_i32(8);
            buf.put_i32(3);
        }
        BackendMessage::ParameterStatus { key, value } => {
            let mut payload = BytesMut::new();
            put_cstring(&mut payload, &key);
            put_cstring(&mut payload, &value);
            buf.put_u8(b'S');
            buf.put_i32((payload.len() + 4) as i32);
            buf.extend_from_slice(&payload);
        }
        BackendMessage::BackendKeyData { pid, secret } => {
            buf.put_u8(b'K');
            buf.put_i32(12);
            buf.put_i32(pid);
            buf.put_i32(secret);
        }
        BackendMessage::ReadyForQuery => {
            buf.put_u8(b'Z');
            buf.put_i32(5);
            buf.put_u8(b'I');
        }
        BackendMessage::RowDescription { fields } => {
            let mut payload = BytesMut::new();
            payload.put_i16(fields.len() as i16);
            for field in fields {
                put_cstring(&mut payload, &field);
                payload.put_i32(0);
                payload.put_i16(0);
                payload.put_i32(25);
                payload.put_i16(-1);
                payload.put_i32(0);
                payload.put_i16(0);
            }
            buf.put_u8(b'T');
            buf.put_i32((payload.len() + 4) as i32);
            buf.extend_from_slice(&payload);
        }
        BackendMessage::DataRow { values } => {
            let mut payload = BytesMut::new();
            payload.put_i16(values.len() as i16);
            for value in values {
                match value {
                    Some(v) => {
                        payload.put_i32(v.len() as i32);
                        payload.extend_from_slice(&v);
                    }
                    None => payload.put_i32(-1),
                }
            }
            buf.put_u8(b'D');
            buf.put_i32((payload.len() + 4) as i32);
            buf.extend_from_slice(&payload);
        }
        BackendMessage::CommandComplete { tag } => {
            let mut payload = BytesMut::new();
            put_cstring(&mut payload, &tag);
            buf.put_u8(b'C');
            buf.put_i32((payload.len() + 4) as i32);
            buf.extend_from_slice(&payload);
        }
        BackendMessage::ErrorResponse { message } => {
            let mut payload = BytesMut::new();
            payload.put_u8(b'M');
            put_cstring(&mut payload, &message);
            payload.put_u8(0);
            buf.put_u8(b'E');
            buf.put_i32((payload.len() + 4) as i32);
            buf.extend_from_slice(&payload);
        }
    }
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

fn put_cstring(buf: &mut BytesMut, value: &str) {
    buf.extend_from_slice(value.as_bytes());
    buf.put_u8(0);
}
