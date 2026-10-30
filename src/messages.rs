use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod send;
pub use send::{SendReceipt, SendRequest, SentAttachment};

const APPLE_EPOCH_UNIX_SECONDS: i64 = 978_307_200;
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;
const SEARCH_SCAN_LIMIT: usize = 5_000;
const MAX_INLINE_ATTACHMENT_BYTES: usize = 5_000_000;
const MENTION_KEY: &[u8] = b"__kIMMentionConfirmedMention";

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
    address_book_paths: Vec<PathBuf>,
    extra_contacts: HashMap<String, String>,
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
pub struct ParticipantInfo {
    pub handle: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatSummary {
    pub row_id: i64,
    pub guid: String,
    pub identifier: Option<String>,
    pub display_name: Option<String>,
    pub service: Option<String>,
    pub participants: Vec<String>,
    pub participant_names: Vec<ParticipantInfo>,
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
pub struct AppliedReaction {
    pub kind: String,
    pub emoji: Option<String>,
    pub sender: Option<String>,
    pub sender_name: Option<String>,
    pub is_from_me: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplySummary {
    pub row_id: i64,
    pub guid: String,
    pub sender: Option<String>,
    pub sender_name: Option<String>,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Mention {
    pub handle: String,
    pub name: Option<String>,
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
    pub sender_name: Option<String>,
    pub service: Option<String>,
    pub is_from_me: bool,
    pub sent_at: Option<String>,
    pub subject: Option<String>,
    pub text: String,
    pub reply_to_guid: Option<String>,
    pub reply_to: Option<ReplySummary>,
    pub mentions: Vec<Mention>,
    pub reaction: Option<Reaction>,
    pub reactions: Vec<AppliedReaction>,
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

#[derive(Debug, Clone, Default)]
struct ContactMap {
    by_key: HashMap<String, String>,
}

impl ContactMap {
    fn insert(&mut self, handle: &str, name: &str) {
        let Some(clean_name) = normalized(Some(name)) else {
            return;
        };
        for key in contact_lookup_keys(handle) {
            self.by_key
                .entry(key)
                .or_insert_with(|| clean_name.to_string());
        }
    }

    fn insert_override(&mut self, handle: &str, name: &str) {
        let Some(clean_name) = normalized(Some(name)) else {
            return;
        };
        for key in contact_lookup_keys(handle) {
            self.by_key.insert(key, clean_name.to_string());
        }
    }

    fn resolve(&self, handle: &str) -> Option<String> {
        for key in contact_lookup_keys(handle) {
            if let Some(name) = self.by_key.get(&key) {
                return Some(name.clone());
            }
        }
        None
    }
}

impl MessagesStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut address_book_paths = Vec::new();
        if let Some(parent) = path.parent() {
            address_book_paths.extend(discover_address_book_paths(parent));
        }
        if let Some(home) = std::env::var_os("HOME") {
            for candidate in discover_address_book_paths(Path::new(&home)) {
                if !address_book_paths.contains(&candidate) {
                    address_book_paths.push(candidate);
                }
            }
        }
        Self {
            path,
            address_book_paths,
            extra_contacts: HashMap::new(),
        }
    }

    pub fn default_for_current_user() -> Result<Self, MessagesError> {
        let home = std::env::var_os("HOME").ok_or(MessagesError::HomeUnavailable)?;
        let home_path = PathBuf::from(home);
        Ok(Self {
            path: home_path.join("Library/Messages/chat.db"),
            address_book_paths: discover_address_book_paths(&home_path),
            extra_contacts: HashMap::new(),
        })
    }

    pub fn with_address_book_paths(mut self, paths: impl IntoIterator<Item = PathBuf>) -> Self {
        self.address_book_paths = paths.into_iter().collect();
        self
    }

    pub fn with_contacts(mut self, contacts: impl IntoIterator<Item = (String, String)>) -> Self {
        for (handle, name) in contacts {
            self.extra_contacts.insert(handle, name);
        }
        self
    }

    pub fn insert_contact(&mut self, handle: impl Into<String>, name: impl Into<String>) {
        self.extra_contacts.insert(handle.into(), name.into());
    }

    pub fn resolve_contact_name(&self, handle: &str) -> Option<String> {
        self.load_contacts().resolve(handle)
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
        let contacts = self.load_contacts();
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
                    participant_names: Vec::new(),
                    latest_at: raw_date.and_then(apple_timestamp_rfc3339),
                    message_count: row.get::<_, i64>(6)?.max(0) as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let participants = participants_by_chat(&conn, &chats)?;
        for chat in &mut chats {
            chat.participants = participants.get(&chat.row_id).cloned().unwrap_or_default();
            chat.participant_names = chat
                .participants
                .iter()
                .map(|handle| ParticipantInfo {
                    handle: handle.clone(),
                    name: contacts.resolve(handle),
                })
                .collect();
            if chat.display_name.is_none() && chat.participant_names.len() == 1 {
                chat.display_name = chat.participant_names[0].name.clone();
            }
        }
        Ok(chats)
    }

    pub fn messages(&self, query: MessageQuery) -> Result<Vec<MessageRecord>, MessagesError> {
        validate_range(&query)?;
        let conn = self.open()?;
        let contacts = self.load_contacts();
        query_messages(&conn, &query, &contacts)
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
        let contacts = self.load_contacts();
        let messages = query_messages(&conn, &query, &contacts)?;
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

    fn load_contacts(&self) -> ContactMap {
        let mut map = ContactMap::default();
        for path in &self.address_book_paths {
            load_address_book_db(path, &mut map);
        }
        for (handle, name) in &self.extra_contacts {
            map.insert_override(handle, name);
        }
        map
    }
}

fn discover_address_book_paths(base_dir: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let roots = [
        base_dir.to_path_buf(),
        base_dir.join("AddressBook"),
        base_dir.join("Library/Application Support/AddressBook"),
    ];
    for root in roots {
        let main_db = root.join("AddressBook-v22.abcddb");
        if main_db.is_file() && !paths.contains(&main_db) {
            paths.push(main_db);
        }
        let sources_dir = root.join("Sources");
        if let Ok(entries) = std::fs::read_dir(sources_dir) {
            let mut source_dbs = entries
                .flatten()
                .map(|entry| entry.path().join("AddressBook-v22.abcddb"))
                .filter(|candidate| candidate.is_file())
                .collect::<Vec<_>>();
            source_dbs.sort();
            for db in source_dbs {
                if !paths.contains(&db) {
                    paths.push(db);
                }
            }
        }
    }
    paths
}

fn load_address_book_db(path: &Path, map: &mut ContactMap) {
    if !path.is_file() {
        return;
    }
    let Ok(conn) = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    ) else {
        return;
    };
    let _ = conn.busy_timeout(Duration::from_secs(1));
    let _ = conn.pragma_update(None, "query_only", true);

    if let Ok(mut stmt) = conn.prepare(
        r#"SELECT r.ZFIRSTNAME, r.ZLASTNAME, r.ZORGANIZATION, r.ZNICKNAME, p.ZFULLNUMBER
           FROM ZABCDRECORD r
           JOIN ZABCDPHONENUMBER p ON p.ZOWNER = r.Z_PK
           WHERE p.ZFULLNUMBER IS NOT NULL AND p.ZFULLNUMBER != ''"#,
    ) {
        if let Ok(rows) = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        }) {
            for row in rows.flatten() {
                let (first, last, org, nickname, phone) = row;
                if let Some(name) = format_contact_name(
                    first.as_deref(),
                    last.as_deref(),
                    org.as_deref(),
                    nickname.as_deref(),
                ) {
                    map.insert(&phone, &name);
                }
            }
        }
    }

    if let Ok(mut stmt) = conn.prepare(
        r#"SELECT r.ZFIRSTNAME, r.ZLASTNAME, r.ZORGANIZATION, r.ZNICKNAME, e.ZADDRESSNORMALIZED
           FROM ZABCDRECORD r
           JOIN ZABCDEMAILADDRESS e ON e.ZOWNER = r.Z_PK
           WHERE e.ZADDRESSNORMALIZED IS NOT NULL AND e.ZADDRESSNORMALIZED != ''"#,
    ) {
        if let Ok(rows) = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        }) {
            for row in rows.flatten() {
                let (first, last, org, nickname, email) = row;
                if let Some(name) = format_contact_name(
                    first.as_deref(),
                    last.as_deref(),
                    org.as_deref(),
                    nickname.as_deref(),
                ) {
                    map.insert(&email, &name);
                }
            }
        }
    };
}

fn format_contact_name(
    first: Option<&str>,
    last: Option<&str>,
    organization: Option<&str>,
    nickname: Option<&str>,
) -> Option<String> {
    let first = normalized(first);
    let last = normalized(last);
    let organization = normalized(organization);
    let nickname = normalized(nickname);
    match (first, last) {
        (Some(f), Some(l)) => Some(format!("{f} {l}")),
        (Some(f), None) => Some(f.to_string()),
        (None, Some(l)) => Some(l.to_string()),
        (None, None) => nickname.or(organization).map(str::to_string),
    }
}

fn contact_lookup_keys(handle: &str) -> Vec<String> {
    let trimmed = handle.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let mut keys = vec![trimmed.to_lowercase()];
    if !trimmed.contains('@') {
        let digits: String = trimmed.chars().filter(char::is_ascii_digit).collect();
        if digits.len() == 10 {
            keys.push(format!("+1{digits}"));
            keys.push(digits);
        } else if digits.len() == 11 && digits.starts_with('1') {
            keys.push(format!("+{digits}"));
            keys.push(digits[1..].to_string());
            keys.push(digits);
        } else if digits.len() >= 7 {
            keys.push(format!("+{digits}"));
            if digits.len() > 10 {
                keys.push(digits[digits.len() - 10..].to_string());
            }
            keys.push(digits);
        }
    }
    keys
}

fn has_column(conn: &Connection, table: &str, column: &str) -> bool {
    let sql = format!("PRAGMA table_info({table})");
    let Ok(mut stmt) = conn.prepare(&sql) else {
        return false;
    };
    let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(1)) else {
        return false;
    };
    let found = rows.flatten().any(|col| col.eq_ignore_ascii_case(column));
    found
}

fn query_messages(
    conn: &Connection,
    query: &MessageQuery,
    contacts: &ContactMap,
) -> Result<Vec<MessageRecord>, MessagesError> {
    let originator_guid_expr = if has_column(conn, "message", "thread_originator_guid") {
        "NULLIF(m.thread_originator_guid, '')"
    } else {
        "NULL"
    };
    let originator_part_expr = if has_column(conn, "message", "thread_originator_part") {
        "NULLIF(m.thread_originator_part, '')"
    } else {
        "NULL"
    };
    let mut sql = format!(
        r#"SELECT m.ROWID, COALESCE(NULLIF(m.guid, ''), 'message-' || m.ROWID),
                  m.date, m.text, m.attributedBody, m.is_from_me,
                  NULLIF(h.id, ''), NULLIF(m.service, ''), NULLIF(m.subject, ''),
                  COALESCE(NULLIF(c.guid, ''), NULLIF(c.chat_identifier, ''), 'chat-' || c.ROWID),
                  NULLIF(c.chat_identifier, ''), NULLIF(c.display_name, ''),
                  NULLIF(m.associated_message_guid, ''), m.associated_message_type,
                  NULLIF(m.associated_message_emoji, ''),
                  {originator_guid_expr}, {originator_part_expr}
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
        let sender: Option<String> = row.get(6)?;
        let sender_name = sender
            .as_deref()
            .and_then(|handle| contacts.resolve(handle));
        let associated_guid: Option<String> = row.get(12)?;
        let associated_type: Option<i64> = row.get(13)?;
        let associated_emoji: Option<String> = row.get(14)?;
        let reply_to_guid: Option<String> = row.get(15)?;
        let _thread_originator_part: Option<String> = row.get(16)?;
        let mentions = attributed
            .as_deref()
            .map(extract_attributed_mentions)
            .unwrap_or_default()
            .into_iter()
            .map(|handle| {
                let name = contacts.resolve(&handle);
                Mention { handle, name }
            })
            .collect();
        messages.push(MessageRecord {
            row_id,
            guid,
            chat_guid: row.get(9)?,
            chat_identifier: row.get(10)?,
            chat_display_name: row.get(11)?,
            sender,
            sender_name,
            service: row.get(7)?,
            is_from_me: row.get::<_, i64>(5)? != 0,
            sent_at: raw_date.and_then(apple_timestamp_rfc3339),
            subject: row.get(8)?,
            text: plain_text
                .filter(|text| !text.is_empty())
                .or_else(|| attributed.as_deref().and_then(extract_attributed_body))
                .unwrap_or_default(),
            reply_to_guid,
            reply_to: None,
            mentions,
            reaction: reaction(associated_guid, associated_type, associated_emoji),
            reactions: Vec::new(),
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

    let reply_guids = messages
        .iter()
        .filter_map(|message| message.reply_to_guid.as_deref())
        .map(|guid| canonical_message_guid(guid).to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let replies = replies_by_guid(conn, &reply_guids, contacts)?;

    let message_guids = messages
        .iter()
        .map(|message| message.guid.clone())
        .collect::<Vec<_>>();
    let mut applied_reactions = reactions_by_target_guid(conn, &message_guids, contacts)?;

    for message in &mut messages {
        message.attachments = attachments.remove(&message.row_id).unwrap_or_default();
        if let Some(reply_guid) = message.reply_to_guid.as_deref() {
            let target_guid = canonical_message_guid(reply_guid);
            message.reply_to = replies.get(target_guid).cloned();
        }
        message.reactions = applied_reactions.remove(&message.guid).unwrap_or_default();
    }
    Ok(messages)
}

fn canonical_message_guid(raw: &str) -> &str {
    let trimmed = raw.trim();
    let after_slash = trimmed.rsplit_once('/').map_or(trimmed, |(_, tail)| tail);
    after_slash.strip_prefix("bp:").unwrap_or(after_slash)
}

fn replies_by_guid(
    conn: &Connection,
    guids: &[String],
    contacts: &ContactMap,
) -> Result<HashMap<String, ReplySummary>, MessagesError> {
    if guids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", guids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        r#"SELECT m.ROWID, COALESCE(NULLIF(m.guid, ''), 'message-' || m.ROWID),
                  m.text, m.attributedBody, NULLIF(h.id, '')
           FROM message m
           LEFT JOIN handle h ON h.ROWID = m.handle_id
           WHERE m.guid IN ({placeholders})"#
    );
    let values = guids
        .iter()
        .cloned()
        .map(SqlValue::Text)
        .collect::<Vec<_>>();
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values), |row| {
        let row_id: i64 = row.get(0)?;
        let guid: String = row.get(1)?;
        let plain_text: Option<String> = row.get(2)?;
        let attributed: Option<Vec<u8>> = row.get(3)?;
        let sender: Option<String> = row.get(4)?;
        let sender_name = sender
            .as_deref()
            .and_then(|handle| contacts.resolve(handle));
        let text = plain_text
            .filter(|text| !text.is_empty())
            .or_else(|| attributed.as_deref().and_then(extract_attributed_body))
            .unwrap_or_default();
        Ok(ReplySummary {
            row_id,
            guid,
            sender,
            sender_name,
            text,
        })
    })?;
    let mut map = HashMap::new();
    for row in rows {
        let summary = row?;
        map.insert(summary.guid.clone(), summary);
    }
    Ok(map)
}

fn reactions_by_target_guid(
    conn: &Connection,
    target_guids: &[String],
    contacts: &ContactMap,
) -> Result<HashMap<String, Vec<AppliedReaction>>, MessagesError> {
    if target_guids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", target_guids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        r#"SELECT m.associated_message_guid,
                  m.associated_message_type,
                  NULLIF(m.associated_message_emoji, ''),
                  m.is_from_me,
                  NULLIF(h.id, '')
           FROM message m
           LEFT JOIN handle h ON h.ROWID = m.handle_id
           WHERE m.associated_message_guid IS NOT NULL
             AND m.associated_message_guid != ''
             AND ((m.associated_message_type BETWEEN 2000 AND 2006)
                  OR (m.associated_message_type BETWEEN 3000 AND 3006))
             AND REPLACE(
                   CASE
                     WHEN INSTR(m.associated_message_guid, '/') > 0
                     THEN SUBSTR(m.associated_message_guid, INSTR(m.associated_message_guid, '/') + 1)
                     ELSE m.associated_message_guid
                   END,
                   'bp:',
                   ''
                 ) IN ({placeholders})
           ORDER BY CAST(m.date AS INTEGER) ASC, m.ROWID ASC"#
    );
    let values = target_guids
        .iter()
        .cloned()
        .map(SqlValue::Text)
        .collect::<Vec<_>>();
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, i64>(3)? != 0,
            row.get::<_, Option<String>>(4)?,
        ))
    })?;

    let mut grouped = HashMap::<String, Vec<AppliedReaction>>::new();
    for row in rows {
        let (raw_target, associated_type, emoji, is_from_me, sender) = row?;
        let target_guid = canonical_message_guid(&raw_target).to_string();
        let Some(parsed) = reaction(Some(target_guid.clone()), Some(associated_type), emoji) else {
            continue;
        };
        if parsed.kind == "unknown" {
            continue;
        }
        let actor_key = reaction_actor_key(sender.as_deref(), is_from_me);
        let entry_list = grouped.entry(target_guid).or_default();
        if parsed.removed {
            entry_list.retain(|existing| {
                !(reaction_actor_key(existing.sender.as_deref(), existing.is_from_me) == actor_key
                    && existing.kind == parsed.kind)
            });
        } else {
            let sender_name = sender
                .as_deref()
                .and_then(|handle| contacts.resolve(handle));
            if let Some(existing) = entry_list.iter_mut().find(|existing| {
                reaction_actor_key(existing.sender.as_deref(), existing.is_from_me) == actor_key
                    && existing.kind == parsed.kind
            }) {
                existing.emoji = parsed.emoji;
            } else {
                entry_list.push(AppliedReaction {
                    kind: parsed.kind,
                    emoji: parsed.emoji,
                    sender,
                    sender_name,
                    is_from_me,
                });
            }
        }
    }
    Ok(grouped)
}

fn reaction_actor_key(sender: Option<&str>, is_from_me: bool) -> String {
    match sender.map(str::trim).filter(|s| !s.is_empty()) {
        Some(handle) => handle.to_lowercase(),
        None if is_from_me => "__me__".to_string(),
        None => "__unknown__".to_string(),
    }
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
    let position = index.checked_add(marker.len() + 5)?;
    read_typedstream_string_at(blob, position)
}

/// Extract confirmed `@mention` handles (`__kIMMentionConfirmedMention`) from
/// Apple's `attributedBody` typedstream blob.
pub fn extract_attributed_mentions(blob: &[u8]) -> Vec<String> {
    let mut mentions = Vec::new();
    let mut seen = HashSet::new();
    let mut search_from = 0;

    while search_from + MENTION_KEY.len() <= blob.len() {
        let Some(rel_idx) = blob[search_from..]
            .windows(MENTION_KEY.len())
            .position(|window| window == MENTION_KEY)
        else {
            break;
        };
        let key_end = search_from + rel_idx + MENTION_KEY.len();
        let next_key = blob[key_end..]
            .windows(5)
            .position(|window| window == b"__kIM")
            .map(|offset| key_end + offset)
            .unwrap_or(blob.len());
        let window_end = next_key.min(key_end.saturating_add(128)).min(blob.len());
        if let Some(handle) = extract_mention_from_slice(&blob[key_end..window_end]) {
            if seen.insert(handle.clone()) {
                mentions.push(handle);
            }
        }
        search_from = key_end;
    }

    mentions
}

fn extract_mention_from_slice(slice: &[u8]) -> Option<String> {
    let ns_marker = b"NSString";
    if let Some(ns_idx) = slice
        .windows(ns_marker.len())
        .position(|window| window == ns_marker)
    {
        if let Some(pos) = ns_idx.checked_add(ns_marker.len() + 5) {
            if let Some(candidate) = read_typedstream_string_at(slice, pos) {
                if is_valid_mention_handle(&candidate) {
                    return Some(candidate);
                }
            }
        }
    }

    for idx in 0..slice.len().min(32) {
        if slice.get(idx..idx + 2) == Some(&[0x01, b'+']) {
            if let Some(candidate) = read_typedstream_string_at(slice, idx + 2) {
                if is_valid_mention_handle(&candidate) {
                    return Some(candidate);
                }
            }
        }
    }

    for idx in 0..slice.len().min(16) {
        if let Some(candidate) = read_typedstream_string_at(slice, idx) {
            if is_valid_mention_handle(&candidate) {
                return Some(candidate);
            }
        }
    }

    None
}

fn is_valid_mention_handle(candidate: &str) -> bool {
    let trimmed = candidate.trim();
    if trimmed.len() < 3 || trimmed.len() > 254 {
        return false;
    }
    if trimmed.starts_with("NS") || trimmed.starts_with("__kIM") {
        return false;
    }
    if trimmed.chars().any(char::is_control) {
        return false;
    }
    if let Some(rest) = trimmed.strip_prefix('+') {
        return rest.len() >= 2 && rest.chars().any(|c| c.is_ascii_digit());
    }
    if let Some((local, domain)) = trimmed.split_once('@') {
        return !local.is_empty() && domain.contains('.');
    }
    if trimmed.starts_with("urn:") || trimmed.starts_with("tel:") || trimmed.starts_with("mailto:")
    {
        return true;
    }
    let digit_count = trimmed.chars().filter(char::is_ascii_digit).count();
    digit_count >= 7 && trimmed.chars().all(|c| c.is_ascii_graphic())
}

fn read_typedstream_string_at(blob: &[u8], mut position: usize) -> Option<String> {
    let length_tag = *blob.get(position)?;
    position += 1;
    let text_length = match length_tag {
        0 => return None,
        value if value < 0x80 => value as usize,
        0x81 => read_little_endian(blob, &mut position, 2)?,
        0x82 => read_little_endian(blob, &mut position, 3)?,
        0x83 => read_little_endian(blob, &mut position, 4)?,
        _ => return None,
    };
    let end = position.checked_add(text_length)?;
    let bytes = blob.get(position..end)?;
    std::str::from_utf8(bytes).ok().map(str::to_owned)
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

    fn attributed_with_mentions(text: &str, mentions: &[&str]) -> Vec<u8> {
        let mut blob = attributed(text);
        for handle in mentions {
            blob.extend_from_slice(&[0x84, 0x01, b'+']);
            blob.push(MENTION_KEY.len() as u8);
            blob.extend_from_slice(MENTION_KEY);
            // Simulate typedstream NSString backreference followed by \x01+ and length-prefixed handle
            blob.extend_from_slice(&[0x86, 0x84, 0x02, 0x94, 0x84, 0x01, b'+']);
            blob.push(handle.len() as u8);
            blob.extend_from_slice(handle.as_bytes());
        }
        blob
    }

    fn create_address_book(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE ZABCDRECORD (
                Z_PK INTEGER PRIMARY KEY,
                ZFIRSTNAME TEXT,
                ZLASTNAME TEXT,
                ZORGANIZATION TEXT,
                ZNICKNAME TEXT
            );
            CREATE TABLE ZABCDPHONENUMBER (
                Z_PK INTEGER PRIMARY KEY,
                ZOWNER INTEGER,
                ZFULLNUMBER TEXT
            );
            CREATE TABLE ZABCDEMAILADDRESS (
                Z_PK INTEGER PRIMARY KEY,
                ZOWNER INTEGER,
                ZADDRESSNORMALIZED TEXT
            );
            INSERT INTO ZABCDRECORD VALUES (1, 'Alice', 'Smith', 'Proxy Inc', NULL);
            INSERT INTO ZABCDPHONENUMBER VALUES (1, 1, '(415) 555-1234');
            INSERT INTO ZABCDEMAILADDRESS VALUES (1, 1, 'alice@example.com');
            INSERT INTO ZABCDRECORD VALUES (2, NULL, NULL, 'Acme Corp', NULL);
            INSERT INTO ZABCDPHONENUMBER VALUES (2, 2, '+1 (650) 555-9876');
            "#,
        )
        .unwrap();
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
                associated_message_emoji TEXT, thread_originator_guid TEXT,
                thread_originator_part TEXT
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
        let ab_path = dir.path().join("AddressBook-v22.abcddb");
        create_address_book(&ab_path);
        let store = MessagesStore::new(path).with_address_book_paths([ab_path]);
        (dir, store)
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
        assert_eq!(
            chats[0].participant_names,
            vec![ParticipantInfo {
                handle: "+14155551234".to_string(),
                name: Some("Alice Smith".to_string()),
            }]
        );

        let messages = store.messages(MessageQuery::default()).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].guid, "msg-new");
        assert_eq!(messages[0].text, "typed body");
        assert_eq!(messages[1].text, "older");
        assert_eq!(messages[1].sender.as_deref(), Some("+14155551234"));
        assert_eq!(messages[1].sender_name.as_deref(), Some("Alice Smith"));
    }

    #[test]
    fn contact_resolution_populates_direct_chat_display_name_and_sources_db() {
        let (dir, store) = fixture();
        let source_dir = dir.path().join("Sources/source-1");
        std::fs::create_dir_all(&source_dir).unwrap();
        let source_ab = source_dir.join("AddressBook-v22.abcddb");
        create_address_book(&source_ab);

        let conn = Connection::open(store.path()).unwrap();
        conn.execute(
            "INSERT INTO chat VALUES (2, 'iMessage;-;+16505559876', '+16505559876', NULL, NULL, 'iMessage')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO handle VALUES (2, '+16505559876', 'iMessage')",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO chat_handle_join VALUES (2, 2)", [])
            .unwrap();
        drop(conn);

        let store = MessagesStore::new(store.path()).with_address_book_paths([source_ab]);
        let chats = store.chats(20, 0).unwrap();
        let direct = chats.iter().find(|c| c.row_id == 2).unwrap();
        assert_eq!(direct.display_name.as_deref(), Some("Acme Corp"));
        assert_eq!(
            direct.participant_names,
            vec![ParticipantInfo {
                handle: "+16505559876".to_string(),
                name: Some("Acme Corp".to_string()),
            }]
        );
    }

    #[test]
    fn inline_reply_context_resolves_originator_with_or_without_part_prefix() {
        let (_dir, store) = fixture();
        let conn = Connection::open(store.path()).unwrap();
        let now = Utc::now();
        conn.execute(
            "INSERT INTO message VALUES (3, 'msg-reply', ?1, 'replying to you', NULL, 1, NULL, 'iMessage', NULL, NULL, NULL, NULL, 'p:0/msg-old', '0:0:5')",
            params![to_apple_nanoseconds(now + ChronoDuration::seconds(10))],
        )
        .unwrap();
        conn.execute("INSERT INTO chat_message_join VALUES (1, 3)", [])
            .unwrap();
        drop(conn);

        let messages = store.messages(MessageQuery::default()).unwrap();
        let reply = messages.iter().find(|m| m.guid == "msg-reply").unwrap();
        assert_eq!(reply.reply_to_guid.as_deref(), Some("p:0/msg-old"));
        assert_eq!(
            reply.reply_to,
            Some(ReplySummary {
                row_id: 1,
                guid: "msg-old".to_string(),
                sender: Some("+14155551234".to_string()),
                sender_name: Some("Alice Smith".to_string()),
                text: "older".to_string(),
            })
        );
    }

    #[test]
    fn attributed_mentions_extracts_handles_and_resolves_contact_names() {
        let (_dir, store) = fixture();
        let blob = attributed_with_mentions(
            "Hey Alice and Bob",
            &["+14155551234", "alice@example.com", "+12125550199"],
        );
        let handles = extract_attributed_mentions(&blob);
        assert_eq!(
            handles,
            vec![
                "+14155551234".to_string(),
                "alice@example.com".to_string(),
                "+12125550199".to_string()
            ]
        );

        let conn = Connection::open(store.path()).unwrap();
        let now = Utc::now();
        conn.execute(
            "INSERT INTO message VALUES (4, 'msg-mention', ?1, NULL, ?2, 0, 1, 'iMessage', NULL, NULL, NULL, NULL, NULL, NULL)",
            params![to_apple_nanoseconds(now + ChronoDuration::seconds(20)), blob],
        )
        .unwrap();
        conn.execute("INSERT INTO chat_message_join VALUES (1, 4)", [])
            .unwrap();
        drop(conn);

        let messages = store.messages(MessageQuery::default()).unwrap();
        let msg = messages.iter().find(|m| m.guid == "msg-mention").unwrap();
        assert_eq!(msg.text, "Hey Alice and Bob");
        assert_eq!(
            msg.mentions,
            vec![
                Mention {
                    handle: "+14155551234".to_string(),
                    name: Some("Alice Smith".to_string()),
                },
                Mention {
                    handle: "alice@example.com".to_string(),
                    name: Some("Alice Smith".to_string()),
                },
                Mention {
                    handle: "+12125550199".to_string(),
                    name: None,
                },
            ]
        );
    }

    #[test]
    fn reactions_aggregate_adds_and_fold_removals_on_target_messages() {
        let (_dir, store) = fixture();
        let conn = Connection::open(store.path()).unwrap();
        let now = Utc::now();
        conn.execute(
            "INSERT INTO handle VALUES (2, 'alice@example.com', 'iMessage')",
            [],
        )
        .unwrap();
        // 1) +14155551234 adds a heart (2000) to p:0/msg-old
        conn.execute(
            "INSERT INTO message VALUES (10, 'tap-1', ?1, 'Loved an image', NULL, 0, 1, 'iMessage', NULL, 'p:0/msg-old', 2000, NULL, NULL, NULL)",
            params![to_apple_nanoseconds(now + ChronoDuration::seconds(1))],
        )
        .unwrap();
        // 2) Me adds a custom emoji (2006) to p:1/msg-old
        conn.execute(
            "INSERT INTO message VALUES (11, 'tap-2', ?1, 'Reacted 🔥', NULL, 1, NULL, 'iMessage', NULL, 'p:1/msg-old', 2006, '🔥', NULL, NULL)",
            params![to_apple_nanoseconds(now + ChronoDuration::seconds(2))],
        )
        .unwrap();
        // 3) alice@example.com adds a like (2001) to msg-old
        conn.execute(
            "INSERT INTO message VALUES (12, 'tap-3', ?1, 'Liked', NULL, 0, 2, 'iMessage', NULL, 'msg-old', 2001, NULL, NULL, NULL)",
            params![to_apple_nanoseconds(now + ChronoDuration::seconds(3))],
        )
        .unwrap();
        // 4) +14155551234 removes heart (3000) from p:0/msg-old
        conn.execute(
            "INSERT INTO message VALUES (13, 'tap-4', ?1, 'Removed a heart', NULL, 0, 1, 'iMessage', NULL, 'p:0/msg-old', 3000, NULL, NULL, NULL)",
            params![to_apple_nanoseconds(now + ChronoDuration::seconds(4))],
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO chat_message_join VALUES (1, 10);
             INSERT INTO chat_message_join VALUES (1, 11);
             INSERT INTO chat_message_join VALUES (1, 12);
             INSERT INTO chat_message_join VALUES (1, 13);",
        )
        .unwrap();
        drop(conn);

        let messages = store.messages(MessageQuery::default()).unwrap();
        let target = messages.iter().find(|m| m.guid == "msg-old").unwrap();
        assert_eq!(
            target.reactions,
            vec![
                AppliedReaction {
                    kind: "emoji".to_string(),
                    emoji: Some("🔥".to_string()),
                    sender: None,
                    sender_name: None,
                    is_from_me: true,
                },
                AppliedReaction {
                    kind: "like".to_string(),
                    emoji: None,
                    sender: Some("alice@example.com".to_string()),
                    sender_name: Some("Alice Smith".to_string()),
                    is_from_me: false,
                },
            ]
        );
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
