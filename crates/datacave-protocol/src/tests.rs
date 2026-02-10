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
    async fn parse_flush_message() {
        let msg = write_frontend_msg(b'H', &[]);
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&msg).await.expect("write");
        let parsed = read_message(&mut server).await.expect("read");
        match parsed {
            FrontendMessage::Flush => {}
            _ => panic!("expected Flush, got {:?}", parsed),
        }
    }

    #[tokio::test]
    async fn parse_extended_query_sequence_including_flush() {
        let mut msgs = Vec::new();
        let mut parse_payload = Vec::new();
        parse_payload.extend_from_slice(b"s\0");
        parse_payload.extend_from_slice(b"SELECT 1\0");
        parse_payload.extend_from_slice(&0i16.to_be_bytes());
        msgs.push(write_frontend_msg(b'P', &parse_payload));
        let mut bind_payload = Vec::new();
        bind_payload.extend_from_slice(b"\0");
        bind_payload.extend_from_slice(b"s\0");
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        bind_payload.extend_from_slice(&0i16.to_be_bytes());
        msgs.push(write_frontend_msg(b'B', &bind_payload));
        let mut describe_payload = Vec::new();
        describe_payload.push(b'S');
        describe_payload.extend_from_slice(b"s\0");
        msgs.push(write_frontend_msg(b'D', &describe_payload));
        let mut execute_payload = Vec::new();
        execute_payload.extend_from_slice(b"\0");
        execute_payload.extend_from_slice(&0i32.to_be_bytes());
        msgs.push(write_frontend_msg(b'E', &execute_payload));
        msgs.push(write_frontend_msg(b'S', &[]));
        msgs.push(write_frontend_msg(b'H', &[]));

        let (mut client, mut server) = tokio::io::duplex(512);
        for m in &msgs {
            client.write_all(m).await.expect("write");
        }

        let parsed = [
            read_message(&mut server).await.expect("read"),
            read_message(&mut server).await.expect("read"),
            read_message(&mut server).await.expect("read"),
            read_message(&mut server).await.expect("read"),
            read_message(&mut server).await.expect("read"),
            read_message(&mut server).await.expect("read"),
        ];
        assert!(matches!(parsed[0], FrontendMessage::Parse { .. }));
        assert!(matches!(parsed[1], FrontendMessage::Bind { .. }));
        assert!(matches!(parsed[2], FrontendMessage::Describe { .. }));
        assert!(matches!(parsed[3], FrontendMessage::Execute { .. }));
        assert!(matches!(parsed[4], FrontendMessage::Sync));
        assert!(matches!(parsed[5], FrontendMessage::Flush));
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

    #[tokio::test]
    async fn write_row_description_with_type_oids() {
        use crate::messages::RowDescriptionField;
        let fields = vec![
            RowDescriptionField::with_type("id", "INT"),
            RowDescriptionField::with_type("name", "TEXT"),
        ];
        let (mut client, mut server) = tokio::io::duplex(128);
        write_message(&mut server, BackendMessage::RowDescription { fields })
            .await
            .expect("write");
        let mut typ = [0u8; 1];
        client.read_exact(&mut typ).await.expect("read type");
        assert_eq!(typ[0], b'T', "RowDescription uses type 'T'");
        let mut len_bytes = [0u8; 4];
        client.read_exact(&mut len_bytes).await.expect("read len");
        let len = i32::from_be_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len.saturating_sub(4)];
        if !payload.is_empty() {
            client.read_exact(&mut payload).await.expect("read payload");
        }
        let ncols = i16::from_be_bytes([payload[0], payload[1]]) as usize;
        assert_eq!(ncols, 2);
        let mut i = 2;
        for (name, expected_oid) in [("id", 23i32), ("name", 25i32)] {
            let nul = payload[i..].iter().position(|&b| b == 0).unwrap();
            assert_eq!(String::from_utf8_lossy(&payload[i..i + nul]), name);
            i += nul + 1;
            i += 4 + 2;
            let oid = i32::from_be_bytes(payload[i..i + 4].try_into().unwrap());
            assert_eq!(oid, expected_oid);
            i += 4 + 2 + 4 + 2;
        }
    }

    #[tokio::test]
    async fn write_parameter_description() {
        let (mut client, mut server) = tokio::io::duplex(64);
        write_message(
            &mut server,
            BackendMessage::ParameterDescription {
                param_oids: vec![25, 23],
            },
        )
        .await
        .expect("write");
        let mut typ = [0u8; 1];
        client.read_exact(&mut typ).await.expect("read type");
        assert_eq!(typ[0], b't', "ParameterDescription uses type 't'");
        let mut len_bytes = [0u8; 4];
        client.read_exact(&mut len_bytes).await.expect("read len");
        let len = i32::from_be_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len.saturating_sub(4)];
        if !payload.is_empty() {
            client.read_exact(&mut payload).await.expect("read payload");
        }
        let nparams = i16::from_be_bytes([payload[0], payload[1]]) as usize;
        assert_eq!(nparams, 2);
        let oid1 = i32::from_be_bytes(payload[2..6].try_into().unwrap());
        let oid2 = i32::from_be_bytes(payload[6..10].try_into().unwrap());
        assert_eq!(oid1, 25);
        assert_eq!(oid2, 23);
    }

    #[tokio::test]
    async fn write_parameter_description_empty() {
        let (mut client, mut server) = tokio::io::duplex(32);
        write_message(
            &mut server,
            BackendMessage::ParameterDescription {
                param_oids: vec![],
            },
        )
        .await
        .expect("write");
        let mut typ = [0u8; 1];
        client.read_exact(&mut typ).await.expect("read type");
        assert_eq!(typ[0], b't');
        let mut len_bytes = [0u8; 4];
        client.read_exact(&mut len_bytes).await.expect("read len");
        let len = i32::from_be_bytes(len_bytes) as usize;
        assert_eq!(len, 6, "length = 4 + 2 (nparams) + 0*4 (oids) = 6");
        let mut payload = vec![0u8; len.saturating_sub(4)];
        if !payload.is_empty() {
            client.read_exact(&mut payload).await.expect("read payload");
        }
        let nparams = i16::from_be_bytes([payload[0], payload[1]]) as usize;
        assert_eq!(nparams, 0);
    }

    #[tokio::test]
    async fn data_type_to_oid_mapping() {
        use crate::messages::data_type_to_oid;
        assert_eq!(data_type_to_oid("INT"), 23);
        assert_eq!(data_type_to_oid("INTEGER"), 23);
        assert_eq!(data_type_to_oid("BIGINT"), 20);
        assert_eq!(data_type_to_oid("TEXT"), 25);
        assert_eq!(data_type_to_oid("VARCHAR"), 25);
        assert_eq!(data_type_to_oid("BOOLEAN"), 16);
        assert_eq!(data_type_to_oid("FLOAT"), 700);
        assert_eq!(data_type_to_oid("DOUBLE"), 701);
        assert_eq!(data_type_to_oid("unknown"), 25);
    }
}
