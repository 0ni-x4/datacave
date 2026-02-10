#[cfg(test)]
mod tests {
    use crate::backend::write_message;
    use crate::frontend::{read_message, read_startup};
    use crate::messages::{BackendMessage, CloseTarget, DescribeTarget, FrontendMessage};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn write_frontend_msg(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(msg_type);
        buf.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

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

    #[tokio::test]
    async fn parse_parse_message() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"stmt1\0");
        payload.extend_from_slice(b"SELECT 1\0");
        payload.extend_from_slice(&0i16.to_be_bytes()); // 0 param OIDs
        let msg = write_frontend_msg(b'P', &payload);
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Parse {
                statement_name,
                query,
                param_oids,
            } => {
                assert_eq!(statement_name, "stmt1");
                assert_eq!(query, "SELECT 1");
                assert!(param_oids.is_empty());
            }
            _ => panic!("expected Parse, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn parse_bind_message() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"\0"); // portal name
        payload.extend_from_slice(b"\0"); // statement name
        payload.extend_from_slice(&0i16.to_be_bytes()); // 0 param format codes
        payload.extend_from_slice(&1i16.to_be_bytes()); // 1 param value
        payload.extend_from_slice(&4i32.to_be_bytes()); // length 4
        payload.extend_from_slice(b"1234"); // value
        payload.extend_from_slice(&0i16.to_be_bytes()); // 0 result format codes
        let msg = write_frontend_msg(b'B', &payload);
        let (mut client, mut server) = tokio::io::duplex(128);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_values,
                ..
            } => {
                assert_eq!(portal_name, "");
                assert_eq!(statement_name, "");
                assert_eq!(param_values.len(), 1);
                assert_eq!(param_values[0].as_deref(), Some(b"1234".as_slice()));
            }
            _ => panic!("expected Bind, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn parse_describe_statement() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"stmt1\0");
        let msg = write_frontend_msg(b'D', &payload);
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Describe { target, name } => {
                assert_eq!(target, DescribeTarget::Statement);
                assert_eq!(name, "stmt1");
            }
            _ => panic!("expected Describe, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn parse_describe_portal() {
        let mut payload = Vec::new();
        payload.push(b'P');
        payload.extend_from_slice(b"\0");
        let msg = write_frontend_msg(b'D', &payload);
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Describe { target, name } => {
                assert_eq!(target, DescribeTarget::Portal);
                assert_eq!(name, "");
            }
            _ => panic!("expected Describe, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn parse_execute_message() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"\0");
        payload.extend_from_slice(&0i32.to_be_bytes()); // max_rows = 0 (no limit)
        let msg = write_frontend_msg(b'E', &payload);
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Execute {
                portal_name,
                max_rows,
            } => {
                assert_eq!(portal_name, "");
                assert_eq!(max_rows, 0);
            }
            _ => panic!("expected Execute, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn parse_sync_message() {
        let msg = write_frontend_msg(b'S', &[]);
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Sync => {}
            _ => panic!("expected Sync, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn write_parse_complete() {
        let (mut client, mut server) = tokio::io::duplex(32);
        write_message(&mut server, BackendMessage::ParseComplete)
            .await
            .expect("write");
        let mut bytes = [0u8; 5];
        client.read_exact(&mut bytes).await.expect("read");
        assert_eq!(bytes[0], b'1');
        assert_eq!(i32::from_be_bytes(bytes[1..5].try_into().unwrap()), 4);
    }

    #[tokio::test]
    async fn write_bind_complete() {
        let (mut client, mut server) = tokio::io::duplex(32);
        write_message(&mut server, BackendMessage::BindComplete)
            .await
            .expect("write");
        let mut bytes = [0u8; 5];
        client.read_exact(&mut bytes).await.expect("read");
        assert_eq!(bytes[0], b'2');
        assert_eq!(i32::from_be_bytes(bytes[1..5].try_into().unwrap()), 4);
    }

    #[tokio::test]
    async fn parse_close_statement() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"stmt1\0");
        let msg = write_frontend_msg(b'C', &payload);
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Close { target, name } => {
                assert_eq!(target, CloseTarget::Statement);
                assert_eq!(name, "stmt1");
            }
            _ => panic!("expected Close, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn parse_close_portal() {
        let mut payload = Vec::new();
        payload.push(b'P');
        payload.extend_from_slice(b"\0");
        let msg = write_frontend_msg(b'C', &payload);
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Close { target, name } => {
                assert_eq!(target, CloseTarget::Portal);
                assert_eq!(name, "");
            }
            _ => panic!("expected Close, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn write_close_complete() {
        let (mut client, mut server) = tokio::io::duplex(32);
        write_message(&mut server, BackendMessage::CloseComplete)
            .await
            .expect("write");
        let mut bytes = [0u8; 5];
        client.read_exact(&mut bytes).await.expect("read");
        assert_eq!(bytes[0], b'3');
        assert_eq!(i32::from_be_bytes(bytes[1..5].try_into().unwrap()), 4);
    }

    #[tokio::test]
    async fn write_no_data() {
        let (mut client, mut server) = tokio::io::duplex(32);
        write_message(&mut server, BackendMessage::NoData)
            .await
            .expect("write");
        let mut bytes = [0u8; 5];
        client.read_exact(&mut bytes).await.expect("read");
        assert_eq!(bytes[0], b'n');
        assert_eq!(i32::from_be_bytes(bytes[1..5].try_into().unwrap()), 4);
    }
}
