use super::*;

const HANDLE: &str = "+15555550123";
const GUID: &str = "iMessage;-;+15555550123";

struct Fixture {
    _dir: tempfile::TempDir,
    writer: Connection,
    store: MessagesStore,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("synthetic-chat.db");
        let writer = Connection::open(&path).unwrap();
        writer.execute_batch(
            "CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, chat_identifier TEXT, room_name TEXT);
             CREATE TABLE message (ROWID INTEGER PRIMARY KEY, text TEXT, is_from_me INTEGER);
             CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);",
        ).unwrap();
        writer
            .execute(
                "INSERT INTO chat VALUES (7, ?1, ?2, NULL)",
                params![GUID, HANDLE],
            )
            .unwrap();
        Self {
            _dir: dir,
            writer,
            store: MessagesStore::new(path),
        }
    }

    fn request(text: &str) -> SendRequest {
        SendRequest {
            recipient: format!("  {HANDLE}\n"),
            text: Some(text.into()),
            attachment_path: None,
        }
    }

    /// Replace only Messages.app's external action. The real sender still
    /// validates, resolves the chat and confirms the outgoing database row.
    fn send_and_confirm(&self, text: &str) -> (SendReceipt, Vec<Vec<u8>>) {
        let mut payloads = Vec::new();
        let receipt = self
            .store
            .send_with_runner(Self::request(text), |script, payload, address| {
                assert_eq!(script, TEXT_TO_CHAT);
                assert_eq!(
                    address, GUID,
                    "recipient normalization and chat lookup stay intact"
                );
                payloads.push(payload.as_bytes().to_vec());
                self.writer
                    .execute(
                        "INSERT INTO message (text, is_from_me) VALUES (?1, 1)",
                        params![payload],
                    )
                    .unwrap();
                self.writer
                    .execute(
                        "INSERT INTO chat_message_join VALUES (7, ?1)",
                        params![self.writer.last_insert_rowid()],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        (receipt, payloads)
    }

    fn assert_exact_delivery(&self, text: &str) {
        let (receipt, payloads) = self.send_and_confirm(text);
        assert!(
            payloads == vec![text.as_bytes().to_vec()],
            "delivery must receive one byte-for-byte copy of the original text"
        );
        assert!(receipt.success);
        assert_eq!(receipt.status, "sent");
        assert_eq!(receipt.delivery_status, "confirmed");
        assert_eq!(receipt.recipient, HANDLE);
        assert_eq!(receipt.chat_guid.as_deref(), Some(GUID));
        assert_eq!(receipt.attachment_status, "not_requested");
        assert!(!receipt.retry_safe);
        assert_eq!(receipt.message_row_ids.len(), 1);
        let stored: String = self
            .writer
            .query_row(
                "SELECT text FROM message WHERE ROWID = ?1",
                params![receipt.message_row_ids[0]],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            stored.as_bytes() == text.as_bytes(),
            "the confirmed outgoing row must preserve the original bytes"
        );
    }
}

#[test]
fn literal_markdown_unicode_and_edge_whitespace_reach_the_delivery_boundary_unchanged() {
    Fixture::new().assert_exact_delivery(
        " \t\n**Draft** [link](https://example.test/a?x=1&y=2)\n`$(literal)` \\\"quotes\\\"\nCafe\u{301} 🙂\r\n  ",
    );
}

#[test]
fn exactly_ten_thousand_original_characters_are_delivered_including_edges() {
    for unit in ["x", "🙂"] {
        let text = format!(" {}\n", unit.repeat(9_998));
        assert_eq!(text.chars().count(), 10_000);
        Fixture::new().assert_exact_delivery(&text);
    }
}

#[test]
fn ten_thousand_and_one_original_characters_are_refused_before_external_delivery() {
    for text in [
        "x".repeat(10_001),
        format!(" {}\n", "x".repeat(9_999)),
        format!(" {}\n", "🙂".repeat(9_999)),
    ] {
        assert_eq!(text.chars().count(), 10_001);
        let fixture = Fixture::new();
        let mut calls = 0;
        let result = fixture
            .store
            .send_with_runner(Fixture::request(&text), |_, _, _| {
                calls += 1;
                Err("mock delivery must not be reached".into())
            });
        assert!(
            matches!(result, Err(MessagesError::InvalidArgument(ref reason)) if reason.contains("10000")),
            "{result:?}"
        );
        assert_eq!(
            calls, 0,
            "no oversized request may cross the delivery boundary"
        );
    }
}

#[test]
fn blank_text_and_invalid_attachments_still_fail_before_external_delivery() {
    for text in ["", " \t\r\n", "\u{2003}\u{3000}"] {
        let fixture = Fixture::new();
        let result = fixture
            .store
            .send_with_runner(Fixture::request(text), |_, _, _| {
                panic!("blank text without an attachment must not send")
            });
        assert!(
            matches!(result, Err(MessagesError::InvalidArgument(ref reason)) if reason.contains("text or attachment_path"))
        );
    }
    let fixture = Fixture::new();
    let mut request = Fixture::request("  keep this text  ");
    request.attachment_path = Some(PathBuf::from("relative.png"));
    let result = fixture.store.send_with_runner(request, |_, _, _| {
        panic!("the complete request must validate before text delivery")
    });
    assert!(
        matches!(result, Err(MessagesError::InvalidArgument(ref reason)) if reason.contains("absolute"))
    );
}

#[test]
fn uncertain_external_delivery_keeps_the_exact_payload_and_does_not_retry() {
    let fixture = Fixture::new();
    let text = "\n  **exact draft**\n";
    let mut payloads = Vec::new();
    let receipt = fixture
        .store
        .send_with_runner(Fixture::request(text), |_, payload, _| {
            payloads.push(payload.as_bytes().to_vec());
            Err("mock Apple-event timeout; outcome unknown".into())
        })
        .unwrap();
    assert_eq!(payloads, vec![text.as_bytes().to_vec()]);
    assert!(!receipt.success);
    assert_eq!(receipt.text_status, "unknown");
    assert_eq!(receipt.delivery_status, "unconfirmed");
    assert!(!receipt.retry_safe);
    assert!(receipt.message_row_ids.is_empty());
}
