#[cfg(test)]
mod tests {
    use crate::backend::write_message;
    use crate::frontend::read_startup;
    use crate::messages::BackendMessage;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn startup_parses_params() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let params = b"user\0alice\0tenant_id\0t1\0\0";
        let len = (params.len() + 8) as i32;
        let protocol = 196608i32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&protocol.to_be_bytes());
        buf.extend_from_slice(params);
        client.write_all(&buf).await.expect("write");
        let msg = read_startup(&mut server).await.expect("read");
        match msg {
            crate::messages::FrontendMessage::Startup { params } => {
                assert_eq!(params.get("user").cloned(), Some("alice".into()));
                assert_eq!(params.get("tenant_id").cloned(), Some("t1".into()));
            }
            _ => panic!("unexpected startup"),
        }
    }

    #[tokio::test]
    async fn write_auth_cleartext_message() {
        let (mut client, mut server) = tokio::io::duplex(32);
        write_message(&mut server, BackendMessage::AuthenticationCleartextPassword)
            .await
            .expect("write");
        let mut bytes = [0u8; 9];
        client.read_exact(&mut bytes).await.expect("read");
        assert_eq!(bytes[0], b'R');
        assert_eq!(i32::from_be_bytes(bytes[1..5].try_into().unwrap()), 8);
        assert_eq!(i32::from_be_bytes(bytes[5..9].try_into().unwrap()), 3);
    }
}
