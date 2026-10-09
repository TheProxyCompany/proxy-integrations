use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const APPLE_EPOCH_UNIX_SECONDS: i64 = 978_307_200;
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;
const SEARCH_SCAN_LIMIT: usize = 5_000;
const MAX_INLINE_ATTACHMENT_BYTES: usize = 5_000_000;

#[derive(Debug, Error)]
pub enum MessagesError {
    #[error("Messages database not found at {0}")]
    DatabaseNotFound(PathBuf),
    #[error("HOME is not available")]
    HomeUnavailable,
    #[error("invalid Messages query: {0}")]
    InvalidArgument(String),
    #[error("Messages database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("attachment file error: {0}")]
    AttachmentIo(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct MessagesStore {
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessReport {
    pub path: PathBuf,
    pub readable: bool,
    pub chat_count: u64,
    pub message_count: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatSummary {
    pub row_id: i64,
    pub guid: String,
    pub identifier: Option<String>,
    pub display_name: Option<String>,
    pub service: Option<String>,
    pub participants: Vec<String>,
    pub latest_at: Option<String>,
    pub message_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reaction {
    pub target_guid: String,
    pub kind: String,
    pub removed: bool,
    pub emoji: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attachment {
    pub id: i64,
    pub message_id: i64,
    pub filename: Option<String>,
    pub path: Option<PathBuf>,
    pub mime_type: Option<String>,
    pub uti: Option<String>,
    pub size_bytes: u64,
    pub exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageRecord {
    pub row_id: i64,
    pub guid: String,
    pub chat_guid: String,
    pub chat_identifier: Option<String>,
    pub chat_display_name: Option<String>,
    pub sender: Option<String>,
    pub service: Option<String>,
    pub is_from_me: bool,
    pub sent_at: Option<String>,
    pub subject: Option<String>,
    pub text: String,
    /// GUID of the message this row replies to, as stored in `message.reply_to_guid`.
    /// Absent when the column is NULL or empty. Not resolved to that message's text.
    #[serde(default)]
    pub reply_to_guid: Option<String>,
    /// GUID of the thread root, as stored in `message.thread_originator_guid`.
    /// Independent of `reply_to_guid`. Absent when the column is NULL or empty.
    #[serde(default)]
    pub thread_originator_guid: Option<String>,
    pub reaction: Option<Reaction>,
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageQuery {
    pub chat: Option<String>,
    pub handle: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for MessageQuery {
    fn default() -> Self {
        Self {
            chat: None,
            handle: None,
            since: None,
            until: None,
            limit: DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttachmentContent {
    pub attachment: Attachment,
    pub base64: Option<String>,
    pub truncated: bool,
}

impl MessagesStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn default_for_current_user() -> Result<Self, MessagesError> {
        let home = std::env::var_os("HOME").ok_or(MessagesError::HomeUnavailable)?;
        Ok(Self::new(
            PathBuf::from(home).join("Library/Messages/chat.db"),
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn check_access(&self) -> Result<AccessReport, MessagesError> {
        let conn = match self.open() {
            Ok(conn) => conn,
            Err(MessagesError::DatabaseNotFound(_)) => {
                return Ok(AccessReport {
                    path: self.path.clone(),
                    readable: false,
                    chat_count: 0,
                    message_count: 0,
                    error: Some("Messages history has not been created on this Mac".to_string()),
                });
            }
            Err(MessagesError::Database(error)) => {
                return Ok(AccessReport {
                    path: self.path.clone(),
                    readable: false,
                    chat_count: 0,
                    message_count: 0,
                    error: Some(format!(
                        "Give Proxy Full Disk Access in System Settings > Privacy & Security > Full Disk Access, then restart Proxy ({error})"
                    )),
                });
            }
            Err(error) => return Err(error),
        };
        let chat_count = conn
            .query_row("SELECT COUNT(*) FROM chat", [], |row| row.get::<_, i64>(0))?
            .max(0) as u64;
        let message_count = conn
            .query_row("SELECT COUNT(*) FROM message", [], |row| {
                row.get::<_, i64>(0)
            })?
            .max(0) as u64;
        Ok(AccessReport {
            path: self.path.clone(),
            readable: true,
            chat_count,
            message_count,
            error: None,
        })
    }

    pub fn chats(&self, limit: usize, offset: usize) -> Result<Vec<ChatSummary>, MessagesError> {
        let conn = self.open()?;
        let limit = bounded_limit(limit);
        let mut statement = conn.prepare(
            r#"SELECT c.ROWID,
                      COALESCE(NULLIF(c.guid, ''), NULLIF(c.chat_identifier, ''), 'chat-' || c.ROWID),
                      NULLIF(c.chat_identifier, ''), NULLIF(c.display_name, ''),
                      NULLIF(c.service_name, ''), MAX(m.date), COUNT(m.ROWID)
               FROM chat c
               LEFT JOIN chat_message_join cmj ON cmj.chat_id = c.ROWID
               LEFT JOIN message m ON m.ROWID = cmj.message_id
               GROUP BY c.ROWID
               ORDER BY MAX(CAST(m.date AS INTEGER)) DESC, c.ROWID DESC
               LIMIT ?1 OFFSET ?2"#,
        )?;
        let mut chats = statement
            .query_map(params![limit as i64, offset as i64], |row| {
                let raw_date: Option<i64> = row.get(5)?;
                Ok(ChatSummary {
                    row_id: row.get(0)?,
                    guid: row.get(1)?,
                    identifier: row.get(2)?,
                    display_name: row.get(3)?,
                    service: row.get(4)?,
                    participants: Vec::new(),
                    latest_at: raw_date.and_then(apple_timestamp_rfc3339),
                    message_count: row.get::<_, i64>(6)?.max(0) as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let participants = participants_by_chat(&conn, &chats)?;
        for chat in &mut chats {
            chat.participants = participants.get(&chat.row_id).cloned().unwrap_or_default();
        }
        Ok(chats)
    }

    pub fn messages(&self, query: MessageQuery) -> Result<Vec<MessageRecord>, MessagesError> {
        validate_range(&query)?;
        let conn = self.open()?;
        query_messages(&conn, &query)
    }

    pub fn search(
        &self,
        term: &str,
        mut query: MessageQuery,
    ) -> Result<Vec<MessageRecord>, MessagesError> {
        let needle = term.trim().to_lowercase();
        if needle.is_empty() {
            return Err(MessagesError::InvalidArgument(
                "search term cannot be empty".to_string(),
            ));
        }
        validate_range(&query)?;
        let requested_limit = bounded_limit(query.limit);
        let requested_offset = query.offset;
        query.limit = SEARCH_SCAN_LIMIT;
        query.offset = 0;
        let conn = self.open()?;
        let messages = query_messages(&conn, &query)?;
        Ok(messages
            .into_iter()
            .filter(|message| {
                message.text.to_lowercase().contains(&needle)
                    || message
                        .subject
                        .as_deref()
                        .is_some_and(|subject| subject.to_lowercase().contains(&needle))
            })
            .skip(requested_offset)
            .take(requested_limit)
            .collect())
    }

    pub fn attachments(&self, query: MessageQuery) -> Result<Vec<Attachment>, MessagesError> {
        let messages = self.messages(query)?;
        Ok(messages
            .into_iter()
            .flat_map(|message| message.attachments)
            .collect())
    }

    pub fn attachment(
        &self,
        attachment_id: i64,
        inline_limit: Option<usize>,
    ) -> Result<AttachmentContent, MessagesError> {
        if attachment_id <= 0 {
            return Err(MessagesError::InvalidArgument(
                "attachment_id must be positive".to_string(),
            ));
        }
        let conn = self.open()?;
        let attachment = attachment_by_id(&conn, attachment_id)?
            .ok_or_else(|| MessagesError::InvalidArgument("attachment not found".to_string()))?;
        let max_bytes = inline_limit.unwrap_or(MAX_INLINE_ATTACHMENT_BYTES);
        let (base64, truncated) = match attachment.path.as_deref().filter(|_| attachment.exists) {
            Some(path) if attachment.size_bytes <= max_bytes as u64 => {
                let bytes = std::fs::read(path)?;
                (
                    Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
                    false,
                )
            }
            Some(_) => (None, true),
            None => (None, false),
        };
        Ok(AttachmentContent {
            attachment,
            base64,
            truncated,
        })
    }

    fn open(&self) -> Result<Connection, MessagesError> {
        if !self.path.is_file() {
            return Err(MessagesError::DatabaseNotFound(self.path.clone()));
        }
        // Read-only without `immutable=1`: Messages writes through WAL, and an
        // immutable connection would silently miss live rows in that WAL.
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        conn.busy_timeout(Duration::from_secs(2))?;
        conn.pragma_update(None, "query_only", true)?;
        Ok(conn)
    }
}

fn query_messages(
    conn: &Connection,
    query: &MessageQuery,
) -> Result<Vec<MessageRecord>, MessagesError> {
    let mut sql = String::from(
        r#"SELECT m.ROWID, COALESCE(NULLIF(m.guid, ''), 'message-' || m.ROWID),
                  m.date, m.text, m.attributedBody, m.is_from_me,
                  NULLIF(h.id, ''), NULLIF(m.service, ''), NULLIF(m.subject, ''),
                  COALESCE(NULLIF(c.guid, ''), NULLIF(c.chat_identifier, ''), 'chat-' || c.ROWID),
                  NULLIF(c.chat_identifier, ''), NULLIF(c.display_name, ''),
                  NULLIF(m.associated_message_guid, ''), m.associated_message_type,
                  NULLIF(m.associated_message_emoji, ''),
                  NULLIF(m.reply_to_guid, ''), NULLIF(m.thread_originator_guid, '')
           FROM message m
           JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
           JOIN chat c ON c.ROWID = cmj.chat_id
           LEFT JOIN handle h ON h.ROWID = m.handle_id
           WHERE 1 = 1"#,
    );
    let mut values = Vec::<SqlValue>::new();

    if let Some(chat) = normalized(query.chat.as_deref()) {
        sql.push_str(" AND (c.guid = ? OR c.chat_identifier = ? OR c.room_name = ?)");
        values.extend([
            SqlValue::Text(chat.to_string()),
            SqlValue::Text(chat.to_string()),
            SqlValue::Text(chat.to_string()),
        ]);
    }
    if let Some(handle) = normalized(query.handle.as_deref()) {
        let digits: String = handle.chars().filter(char::is_ascii_digit).collect();
        sql.push_str(" AND (LOWER(h.id) = LOWER(?)");
        values.push(SqlValue::Text(handle.to_string()));
        if digits.len() >= 7 {
            sql.push_str(" OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(h.id, '+', ''), '-', ''), '(', ''), ')', ''), ' ', '') LIKE ?");
            values.push(SqlValue::Text(format!("%{digits}")));
        }
        sql.push(')');
    }
    if let Some(since) = query.since {
        sql.push_str(&format!(" AND {} >= ?", normalized_date_sql("m.date")));
        values.push(SqlValue::Integer(to_apple_nanoseconds(since)));
    }
    if let Some(until) = query.until {
        sql.push_str(&format!(" AND {} < ?", normalized_date_sql("m.date")));
        values.push(SqlValue::Integer(to_apple_nanoseconds(until)));
    }
    sql.push_str(" ORDER BY CAST(m.date AS INTEGER) DESC, m.ROWID DESC LIMIT ? OFFSET ?");
    values.push(SqlValue::Integer(bounded_limit(query.limit) as i64));
    values.push(SqlValue::Integer(query.offset as i64));

    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(params_from_iter(values))?;
    let mut messages = Vec::new();
    let mut seen = HashSet::new();
    while let Some(row) = rows.next()? {
        let row_id: i64 = row.get(0)?;
        let guid: String = row.get(1)?;
        if !seen.insert(guid.clone()) {
            continue;
        }
        let raw_date: Option<i64> = row.get(2)?;
        let plain_text: Option<String> = row.get(3)?;
        let attributed: Option<Vec<u8>> = row.get(4)?;
        let associated_guid: Option<String> = row.get(12)?;
        let associated_type: Option<i64> = row.get(13)?;
        let associated_emoji: Option<String> = row.get(14)?;
        messages.push(MessageRecord {
            row_id,
            guid,
            chat_guid: row.get(9)?,
            chat_identifier: row.get(10)?,
            chat_display_name: row.get(11)?,
            sender: row.get(6)?,
            service: row.get(7)?,
            is_from_me: row.get::<_, i64>(5)? != 0,
            sent_at: raw_date.and_then(apple_timestamp_rfc3339),
            subject: row.get(8)?,
            text: plain_text
                .filter(|text| !text.is_empty())
                .or_else(|| attributed.as_deref().and_then(extract_attributed_body))
                .unwrap_or_default(),
            reply_to_guid: row.get(15)?,
            thread_originator_guid: row.get(16)?,
            reaction: reaction(associated_guid, associated_type, associated_emoji),
            attachments: Vec::new(),
        });
    }
    drop(rows);
    drop(statement);

    let message_ids = messages
        .iter()
        .map(|message| message.row_id)
        .collect::<Vec<_>>();
    let mut attachments = attachments_by_message(conn, &message_ids)?;
    for message in &mut messages {
        message.attachments = attachments.remove(&message.row_id).unwrap_or_default();
    }
    Ok(messages)
}

fn participants_by_chat(
    conn: &Connection,
    chats: &[ChatSummary],
) -> Result<HashMap<i64, Vec<String>>, MessagesError> {
    if chats.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", chats.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT chj.chat_id, h.id FROM chat_handle_join chj JOIN handle h ON h.ROWID = chj.handle_id WHERE chj.chat_id IN ({placeholders}) ORDER BY chj.chat_id, h.id"
    );
    let ids = chats
        .iter()
        .map(|chat| SqlValue::Integer(chat.row_id))
        .collect::<Vec<_>>();
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(ids), |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut participants = HashMap::<i64, Vec<String>>::new();
    for row in rows {
        let (chat_id, handle) = row?;
        participants.entry(chat_id).or_default().push(handle);
    }
    Ok(participants)
}

fn attachments_by_message(
    conn: &Connection,
    message_ids: &[i64],
) -> Result<HashMap<i64, Vec<Attachment>>, MessagesError> {
    if message_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", message_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        r#"SELECT a.ROWID, maj.message_id, a.transfer_name, a.filename,
                  a.mime_type, a.uti, a.total_bytes
           FROM message_attachment_join maj
           JOIN attachment a ON a.ROWID = maj.attachment_id
           WHERE maj.message_id IN ({placeholders})
             AND COALESCE(a.is_sticker, 0) = 0
             AND COALESCE(a.uti, '') NOT LIKE 'com.apple.messages.MSMessageExtensionBalloonPlugin:%'
             AND COALESCE(a.transfer_name, '') NOT LIKE '%.pluginPayloadAttachment'
           ORDER BY a.ROWID"#,
    );
    let values = message_ids
        .iter()
        .copied()
        .map(SqlValue::Integer)
        .collect::<Vec<_>>();
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values), shape_attachment)?;
    let mut grouped = HashMap::<i64, Vec<Attachment>>::new();
    for row in rows {
        let attachment = row?;
        grouped
            .entry(attachment.message_id)
            .or_default()
            .push(attachment);
    }
    Ok(grouped)
}

fn attachment_by_id(
    conn: &Connection,
    attachment_id: i64,
) -> Result<Option<Attachment>, MessagesError> {
    let mut statement = conn.prepare(
        r#"SELECT a.ROWID, maj.message_id, a.transfer_name, a.filename,
                  a.mime_type, a.uti, a.total_bytes
           FROM attachment a
           JOIN message_attachment_join maj ON maj.attachment_id = a.ROWID
           WHERE a.ROWID = ?1 LIMIT 1"#,
    )?;
    let mut rows = statement.query(params![attachment_id])?;
    rows.next()?
        .map(shape_attachment)
        .transpose()
        .map_err(Into::into)
}

fn shape_attachment(row: &rusqlite::Row<'_>) -> rusqlite::Result<Attachment> {
    let transfer_name: Option<String> = row.get(2)?;
    let raw_path: Option<String> = row.get(3)?;
    let path = raw_path.as_deref().map(expand_home);
    let exists = path.as_deref().is_some_and(Path::is_file);
    let database_size = row.get::<_, Option<i64>>(6)?.unwrap_or(0).max(0) as u64;
    let size_bytes = path
        .as_deref()
        .filter(|_| exists)
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .unwrap_or(database_size);
    let filename = transfer_name.or_else(|| {
        path.as_deref()
            .and_then(Path::file_name)
            .map(|name| name.to_string_lossy().into_owned())
    });
    Ok(Attachment {
        id: row.get(0)?,
        message_id: row.get(1)?,
        filename,
        path,
        mime_type: row.get(4)?,
        uti: row.get(5)?,
        size_bytes,
        exists,
    })
}

fn validate_range(query: &MessageQuery) -> Result<(), MessagesError> {
    if query
        .since
        .zip(query.until)
        .is_some_and(|(since, until)| since >= until)
    {
        return Err(MessagesError::InvalidArgument(
            "since must be earlier than until".to_string(),
        ));
    }
    Ok(())
}

fn normalized(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn bounded_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_LIMIT)
}

fn normalized_date_sql(column: &str) -> String {
    format!(
        "CASE WHEN ABS(CAST({column} AS INTEGER)) < 100000000000 THEN CAST({column} AS INTEGER) * 1000000000 ELSE CAST({column} AS INTEGER) END"
    )
}

pub fn to_apple_nanoseconds(date: DateTime<Utc>) -> i64 {
    let seconds = date.timestamp() - APPLE_EPOCH_UNIX_SECONDS;
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(date.timestamp_subsec_nanos() as i64)
}

pub fn apple_timestamp_rfc3339(raw: i64) -> Option<String> {
    let (seconds, nanos) = if raw.unsigned_abs() < 100_000_000_000 {
        (raw, 0)
    } else {
        (
            raw.div_euclid(1_000_000_000),
            raw.rem_euclid(1_000_000_000) as u32,
        )
    };
    Utc.timestamp_opt(APPLE_EPOCH_UNIX_SECONDS.saturating_add(seconds), nanos)
        .single()
        .map(|date| date.to_rfc3339())
}

fn reaction(
    target_guid: Option<String>,
    associated_type: Option<i64>,
    emoji: Option<String>,
) -> Option<Reaction> {
    let target_guid = target_guid?;
    let associated_type = associated_type?;
    let removed = associated_type >= 3000;
    let base = if removed {
        associated_type - 1000
    } else {
        associated_type
    };
    let kind = match base {
        2000 => "heart",
        2001 => "like",
        2002 => "dislike",
        2003 => "laugh",
        2004 => "emphasis",
        2005 => "question",
        2006 => "emoji",
        _ => "unknown",
    }
    .to_string();
    Some(Reaction {
        target_guid,
        kind,
        removed,
        emoji,
    })
}

fn expand_home(path: &str) -> PathBuf {
    if let Some(relative) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(relative);
        }
    }
    PathBuf::from(path)
}

/// Extract the string payload from Apple's attributed-body typedstream.
///
/// Adapted from the MIT-licensed `mac_messages_mcp` 0.9.2 parser by Carter
/// LaSalle. See the repository NOTICE for source attribution.
pub fn extract_attributed_body(blob: &[u8]) -> Option<String> {
    let marker = b"NSString";
    let index = blob
        .windows(marker.len())
        .position(|window| window == marker)?;
    let mut position = index.checked_add(marker.len() + 5)?;
    let length_tag = *blob.get(position)?;
    position += 1;
    let text_length = match length_tag {
        value if value < 0x80 => value as usize,
        0x81 => read_little_endian(blob, &mut position, 2)?,
        0x82 => read_little_endian(blob, &mut position, 3)?,
        0x83 => read_little_endian(blob, &mut position, 4)?,
        _ => return None,
    };
    let end = position.checked_add(text_length)?;
    let bytes = blob.get(position..end)?;
    Some(String::from_utf8_lossy(bytes).into_owned())
}

fn read_little_endian(blob: &[u8], position: &mut usize, width: usize) -> Option<usize> {
    let end = position.checked_add(width)?;
    let bytes = blob.get(*position..end)?;
    *position = end;
    Some(
        bytes
            .iter()
            .enumerate()
            .fold(0usize, |value, (index, byte)| {
                value | ((*byte as usize) << (index * 8))
            }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    fn attributed(text: &str) -> Vec<u8> {
        let mut blob = b"prefixNSString".to_vec();
        blob.extend_from_slice(&[1, 0, 0x84, 1, b'+']);
        if text.len() < 0x80 {
            blob.push(text.len() as u8);
        } else {
            blob.push(0x81);
            blob.extend_from_slice(&(text.len() as u16).to_le_bytes());
        }
        blob.extend_from_slice(text.as_bytes());
        blob
    }

    fn fixture() -> (tempfile::TempDir, MessagesStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chat.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, chat_identifier TEXT, room_name TEXT, display_name TEXT, service_name TEXT);
            CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT, service TEXT);
            CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
            CREATE TABLE message (
                ROWID INTEGER PRIMARY KEY, guid TEXT, date INTEGER, text TEXT, attributedBody BLOB,
                is_from_me INTEGER, handle_id INTEGER, service TEXT, subject TEXT,
                associated_message_guid TEXT, associated_message_type INTEGER,
                associated_message_emoji TEXT, reply_to_guid TEXT, thread_originator_guid TEXT
            );
            CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
            CREATE TABLE attachment (
                ROWID INTEGER PRIMARY KEY, transfer_name TEXT, filename TEXT, mime_type TEXT,
                uti TEXT, total_bytes INTEGER, is_sticker INTEGER
            );
            CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
            INSERT INTO chat VALUES (1, 'iMessage;-;chat123', 'chat123', 'chat123', 'Friends', 'iMessage');
            INSERT INTO handle VALUES (1, '+14155551234', 'iMessage');
            INSERT INTO chat_handle_join VALUES (1, 1);
            "#,
        )
        .unwrap();
        let now = Utc::now();
        conn.execute(
            "INSERT INTO message VALUES (1, 'msg-old', ?1, 'older', NULL, 0, 1, 'iMessage', NULL, NULL, NULL, NULL, NULL, NULL)",
            params![to_apple_nanoseconds(now - ChronoDuration::minutes(2))],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message VALUES (2, 'msg-new', ?1, NULL, ?2, 1, NULL, 'iMessage', NULL, NULL, NULL, NULL, NULL, NULL)",
            params![to_apple_nanoseconds(now), attributed("typed body")],
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO chat_message_join VALUES (1, 1); INSERT INTO chat_message_join VALUES (1, 2);",
        )
        .unwrap();
        drop(conn);
        (dir, MessagesStore::new(path))
    }

    #[test]
    fn attributed_body_supports_short_and_extended_lengths() {
        assert_eq!(
            extract_attributed_body(&attributed("hello")).as_deref(),
            Some("hello")
        );
        let long = "x".repeat(300);
        assert_eq!(
            extract_attributed_body(&attributed(&long)).as_deref(),
            Some(long.as_str())
        );
        assert_eq!(extract_attributed_body(b"not a typedstream"), None);
    }

    #[test]
    fn apple_timestamp_round_trips_current_dates() {
        let now = Utc::now();
        let encoded = to_apple_nanoseconds(now);
        let decoded = DateTime::parse_from_rfc3339(&apple_timestamp_rfc3339(encoded).unwrap())
            .unwrap()
            .with_timezone(&Utc);
        assert!((decoded - now).num_milliseconds().abs() <= 1);
    }

    #[test]
    fn read_only_fixture_returns_stable_newest_first_records() {
        let (_dir, store) = fixture();
        let access = store.check_access().unwrap();
        assert_eq!(access.chat_count, 1);
        assert_eq!(access.message_count, 2);
        assert_eq!(access.error, None);

        let chats = store.chats(20, 0).unwrap();
        assert_eq!(chats.len(), 1);
        assert_eq!(chats[0].guid, "iMessage;-;chat123");
        assert_eq!(chats[0].participants, vec!["+14155551234"]);

        let messages = store.messages(MessageQuery::default()).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].guid, "msg-new");
        assert_eq!(messages[0].text, "typed body");
        assert_eq!(messages[1].text, "older");
    }

    #[test]
    fn access_check_reports_missing_history_without_failing_the_tool() {
        let dir = tempfile::tempdir().unwrap();
        let store = MessagesStore::new(dir.path().join("missing-chat.db"));

        let access = store.check_access().unwrap();

        assert!(!access.readable);
        assert_eq!(access.chat_count, 0);
        assert!(access.error.unwrap().contains("has not been created"));
    }

    #[test]
    fn filters_and_search_share_the_typed_body_path() {
        let (_dir, store) = fixture();
        let query = MessageQuery {
            chat: Some("chat123".to_string()),
            handle: None,
            since: Some(Utc::now() - ChronoDuration::hours(1)),
            until: None,
            limit: 10,
            offset: 0,
        };
        let messages = store.messages(query.clone()).unwrap();
        assert_eq!(messages.len(), 2);
        let matches = store.search("typed", query).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].guid, "msg-new");
    }

    #[test]
    fn recent_and_search_surface_reply_and_thread_guids() {
        let (_dir, store) = fixture();
        let conn = Connection::open(store.path()).unwrap();
        conn.execute(
            "UPDATE message SET reply_to_guid = ?1, thread_originator_guid = ?2 WHERE guid = 'msg-new'",
            params!["msg-old", "thread-root"],
        )
        .unwrap();
        drop(conn);

        let query = MessageQuery {
            limit: 1,
            ..MessageQuery::default()
        };
        for records in [
            store.messages(query.clone()).unwrap(),
            store.search("typed", query).unwrap(),
        ] {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].guid, "msg-new");
            assert_eq!(records[0].reply_to_guid.as_deref(), Some("msg-old"));
            assert_eq!(
                records[0].thread_originator_guid.as_deref(),
                Some("thread-root")
            );
            let json = serde_json::to_value(&records).unwrap();
            assert_eq!(json[0]["reply_to_guid"], "msg-old");
            assert_eq!(json[0]["thread_originator_guid"], "thread-root");
        }
    }

    #[test]
    fn date_bounds_are_inclusive_since_and_exclusive_until() {
        let (_dir, store) = fixture();
        let now = Utc::now();
        let messages = store
            .messages(MessageQuery {
                chat: None,
                handle: None,
                since: Some(now - ChronoDuration::minutes(1)),
                until: Some(now + ChronoDuration::seconds(1)),
                limit: 10,
                offset: 0,
            })
            .unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].guid, "msg-new");
    }
}
