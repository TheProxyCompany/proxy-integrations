//! Sends through Messages.app. The chat is resolved in chat.db, Messages.app
//! is asked over `osascript` with the text and target as argv, and chat.db is
//! read again to confirm the outgoing row landed in that chat.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use super::{MessagesError, MessagesStore};

const MAX_TEXT_CHARS: usize = 10_000;
const MAX_ATTACHMENT_BYTES: u64 = 20_000_000;
const OSASCRIPT_TIMEOUT: Duration = Duration::from_secs(20);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const STAGING_PREFIX: &str = "proxy-message-";
const STAGING_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

const TEXT_TO_CHAT: &str = "on run argv
tell application \"Messages\" to send (item 1 of argv) to chat id (item 2 of argv)
end run";
const TEXT_TO_PARTICIPANT: &str = "on run argv
tell application \"Messages\" to send (item 1 of argv) to participant (item 2 of argv) of 1st account whose service type is iMessage
end run";
const FILE_TO_CHAT: &str = "on run argv
tell application \"Messages\" to send (POSIX file (item 1 of argv)) to chat id (item 2 of argv)
end run";
const FILE_TO_PARTICIPANT: &str = "on run argv
tell application \"Messages\" to send (POSIX file (item 1 of argv)) to participant (item 2 of argv) of 1st account whose service type is iMessage
end run";

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SendRequest {
    pub recipient: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub attachment_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SentAttachment {
    pub path: PathBuf,
    pub mime_type: &'static str,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SendReceipt {
    pub success: bool,
    pub recipient: String,
    pub chat_guid: Option<String>,
    pub transport: &'static str,
    /// `sent`, `not_sent`, `partial_failure` or `unconfirmed`.
    pub status: &'static str,
    /// `sent`, `not_requested`, `not_sent` or `unknown`.
    pub text_status: &'static str,
    pub attachment_status: &'static str,
    /// `confirmed` when the outgoing row was read back from chat.db.
    pub delivery_status: &'static str,
    pub message_row_ids: Vec<i64>,
    pub retry_safe: bool,
    pub attachment: Option<SentAttachment>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Chat { row_id: i64, guid: String },
    Participant(String),
}

#[derive(Debug)]
struct Validated {
    recipient: String,
    text: Option<String>,
    attachment: Option<(PathBuf, &'static str, u64, File)>,
}

impl MessagesStore {
    pub fn send(&self, request: SendRequest) -> Result<SendReceipt, MessagesError> {
        let mut request = validate(request)?;
        let conn = self.open()?;
        let target = resolve_target(&conn, &request.recipient)?;
        let chat_guid = match &target {
            Target::Chat { guid, .. } => Some(guid.clone()),
            Target::Participant(_) => None,
        };
        let mut receipt = SendReceipt {
            success: false,
            recipient: request.recipient.clone(),
            chat_guid,
            transport: "Messages",
            status: "not_sent",
            text_status: if request.text.is_some() {
                "not_sent"
            } else {
                "not_requested"
            },
            attachment_status: if request.attachment.is_some() {
                "not_sent"
            } else {
                "not_requested"
            },
            delivery_status: "unconfirmed",
            message_row_ids: Vec::new(),
            retry_safe: false,
            attachment: request
                .attachment
                .as_ref()
                .map(|(path, mime_type, size_bytes, _)| SentAttachment {
                    path: path.clone(),
                    mime_type,
                    size_bytes: *size_bytes,
                }),
            error: None,
        };

        let baseline = latest_message_row_id(&conn)?;
        if let Some(text) = &request.text {
            let script = match target {
                Target::Chat { .. } => TEXT_TO_CHAT,
                Target::Participant(_) => TEXT_TO_PARTICIPANT,
            };
            if let Err(error) = send_with_messages(script, text, &target) {
                receipt.text_status = "unknown";
                receipt.error = Some(error);
                return Ok(receipt);
            }
            receipt.text_status = "sent";
        }
        if let Some((path, _, _, source)) = &mut request.attachment {
            let staged = match stage(source, path) {
                Ok(staged) => staged,
                Err(error) => {
                    receipt.status = if receipt.text_status == "sent" {
                        "partial_failure"
                    } else {
                        "not_sent"
                    };
                    receipt.error = Some(error.to_string());
                    return Ok(receipt);
                }
            };
            let script = match target {
                Target::Chat { .. } => FILE_TO_CHAT,
                Target::Participant(_) => FILE_TO_PARTICIPANT,
            };
            if let Err(error) = send_with_messages(script, &staged.to_string_lossy(), &target) {
                receipt.attachment_status = "unknown";
                receipt.status = if receipt.text_status == "sent" {
                    "partial_failure"
                } else {
                    "not_sent"
                };
                receipt.error = Some(error);
                return Ok(receipt);
            }
            receipt.attachment_status = "sent";
        }

        let expected =
            usize::from(request.text.is_some()) + usize::from(request.attachment.is_some());
        receipt.message_row_ids = confirm(&conn, &target, baseline, expected)?;
        if receipt.message_row_ids.len() >= expected {
            receipt.success = true;
            receipt.status = "sent";
            receipt.delivery_status = "confirmed";
        } else {
            receipt.status = "unconfirmed";
            receipt.error = Some(format!(
                "Messages accepted the send but {} of {} outgoing rows appeared in chat.db within {}s",
                receipt.message_row_ids.len(),
                expected,
                CONFIRM_TIMEOUT.as_secs()
            ));
        }
        Ok(receipt)
    }
}

fn validate(request: SendRequest) -> Result<Validated, MessagesError> {
    let recipient = request.recipient.trim().to_string();
    if recipient.is_empty() {
        return Err(MessagesError::InvalidArgument(
            "recipient cannot be empty".into(),
        ));
    }
    let text = match request.text {
        Some(text) => {
            let trimmed = text.trim().to_string();
            if trimmed.chars().count() > MAX_TEXT_CHARS {
                return Err(MessagesError::InvalidArgument(format!(
                    "text must be at most {MAX_TEXT_CHARS} characters"
                )));
            }
            (!trimmed.is_empty()).then_some(trimmed)
        }
        None => None,
    };
    let attachment = request
        .attachment_path
        .map(|path| validate_attachment(&path))
        .transpose()?;
    if text.is_none() && attachment.is_none() {
        return Err(MessagesError::InvalidArgument(
            "text or attachment_path is required".into(),
        ));
    }
    Ok(Validated {
        recipient,
        text,
        attachment,
    })
}

const FORBIDDEN_ATTACHMENT_DIRS: &[&str] =
    &[".ssh", ".aws", ".azure", ".gnupg", ".kube", "keychains"];

/// The file must be a regular local JPEG, PNG, GIF, MP4 or MOV whose header
/// matches its extension. Messages.app checks the media again when it imports.
///
/// The open is the check. The leaf is opened with `O_NOFOLLOW`, its kernel
/// resolved path is checked against the denylist so a symlink ancestor naming a
/// credential directory is refused, and the opened handle is retained for
/// [`stage`] so a symlink or file swapped in after validation is never opened.
fn validate_attachment(path: &Path) -> Result<(PathBuf, &'static str, u64, File), MessagesError> {
    let invalid =
        |message: &str| MessagesError::InvalidArgument(format!("attachment_path {message}"));
    if !path.is_absolute() {
        return Err(invalid("must be an absolute local file path"));
    }
    let hidden_caller_leaf = path
        .file_name()
        .map(|name| name.to_string_lossy().starts_with('.'))
        .unwrap_or(true);
    if hidden_caller_leaf || attachment_path_is_forbidden(path) {
        return Err(invalid("must not name a hidden or credential file"));
    }
    let mut file = open_regular_nofollow(path)
        .map_err(|_| invalid("must be a regular file, not a symlink or directory"))?;
    let resolved =
        resolved_path(&file).map_err(|_| invalid("must be an existing readable file"))?;
    let hidden_file = resolved
        .file_name()
        .map(|name| name.to_string_lossy().starts_with('.'))
        .unwrap_or(true);
    if hidden_file || attachment_path_is_forbidden(&resolved) {
        return Err(invalid("must not name a hidden or credential file"));
    }
    let metadata = file
        .metadata()
        .map_err(|_| invalid("must be an existing readable file"))?;
    if !metadata.is_file() {
        return Err(invalid(
            "must be a regular file, not a symlink or directory",
        ));
    }
    let size = metadata.len();
    if size == 0 || size > MAX_ATTACHMENT_BYTES {
        return Err(invalid(&format!(
            "must contain 1...{MAX_ATTACHMENT_BYTES} bytes"
        )));
    }
    let extension = resolved
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let mut header = [0u8; 12];
    let read = file
        .read(&mut header)
        .map_err(|_| invalid("must be readable"))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|_| invalid("must be readable"))?;
    let header = &header[..read];
    let mime_type = match extension.as_str() {
        "jpg" | "jpeg" if header.starts_with(&[0xFF, 0xD8, 0xFF]) => "image/jpeg",
        "png" if header.starts_with(&[0x89, b'P', b'N', b'G']) => "image/png",
        "gif" if header.starts_with(b"GIF8") => "image/gif",
        "mp4" if header.len() >= 8 && &header[4..8] == b"ftyp" => "video/mp4",
        "mov" if header.len() >= 8 && &header[4..8] == b"ftyp" => "video/quicktime",
        _ => {
            return Err(invalid(
                "must be a JPEG, PNG, GIF, MP4 or MOV whose contents match its extension",
            ))
        }
    };
    Ok((path.to_path_buf(), mime_type, size, file))
}

/// The denylist, checked on both the caller path and the path the kernel opened.
/// A symlink ancestor resolves to the denied directory, so the caller's spelling
/// no longer decides what the check sees.
fn attachment_path_is_forbidden(path: &Path) -> bool {
    path.components().any(|component| match component {
        Component::Normal(name) => {
            FORBIDDEN_ATTACHMENT_DIRS.contains(&name.to_string_lossy().to_lowercase().as_str())
        }
        _ => false,
    }) || path.starts_with("/etc")
        || path.starts_with("/private/etc")
}

/// Opens the leaf without following a symlink there. The ancestor walk still
/// follows, and [`resolved_path`] reads back where it landed so the denylist
/// sees the real directory instead of the one the caller spelled.
fn open_regular_nofollow(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    const O_NOFOLLOW: i32 = 0x0100;
    File::options()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(path)
}

/// Where the open landed, after every ancestor symlink. macOS `F_GETPATH`
/// returns the resolved path of the descriptor, so a file swapped in between
/// the open and this read is the file the descriptor holds, not the one the
/// name pointed at afterwards.
fn resolved_path(file: &File) -> std::io::Result<PathBuf> {
    use std::os::unix::io::AsRawFd;
    const F_GETPATH: i32 = 50;
    let mut buf = [0u8; 4096];
    let rc = unsafe { libc_fcntl(file.as_raw_fd(), F_GETPATH, buf.as_mut_ptr()) };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let end = buf.iter().position(|byte| *byte == 0).unwrap_or(buf.len());
    Ok(PathBuf::from(
        String::from_utf8_lossy(&buf[..end]).into_owned(),
    ))
}

unsafe fn libc_fcntl(fd: i32, cmd: i32, path: *mut u8) -> i32 {
    extern "C" {
        fn fcntl(fd: i32, cmd: i32, ...) -> i32;
    }
    fcntl(fd, cmd, path)
}

/// Finds the chat a recipient names: a chat row id, `chat_identifier`, GUID in
/// any service or polarity spelling, or `room_name`. A phone number or email
/// with a direct chat resolves to that chat; without one it is a participant.
pub fn resolve_target(conn: &Connection, recipient: &str) -> Result<Target, MessagesError> {
    let mut candidates = vec![recipient.to_string()];
    let tail = recipient.rsplit(';').next().unwrap_or(recipient);
    for service in ["any", "iMessage", "SMS", "RCS"] {
        for polarity in ["+", "-"] {
            candidates.push(format!("{service};{polarity};{tail}"));
        }
    }
    let row_id = recipient.parse::<i64>().ok();
    let mut statement = conn.prepare(
        "SELECT ROWID, COALESCE(NULLIF(guid, ''), NULLIF(chat_identifier, ''))
         FROM chat
         WHERE (?1 IS NOT NULL AND ROWID = ?1) OR guid = ?2 OR chat_identifier = ?2 OR room_name = ?2
         ORDER BY ROWID DESC LIMIT 1",
    )?;
    for candidate in &candidates {
        let found = statement
            .query_row(params![row_id, candidate], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .optional()?;
        if let Some((row_id, Some(guid))) = found {
            return Ok(Target::Chat { row_id, guid });
        }
    }
    if recipient.contains(';') || tail.starts_with("chat") {
        return Err(MessagesError::InvalidArgument(format!(
            "no chat named {recipient} in Messages; use a guid from messages_list_chats"
        )));
    }
    Ok(Target::Participant(recipient.to_string()))
}

fn send_with_messages(script: &str, payload: &str, target: &Target) -> Result<(), String> {
    let address = match target {
        Target::Chat { guid, .. } => guid.as_str(),
        Target::Participant(handle) => handle.as_str(),
    };
    match run_osascript(script, payload, address) {
        Err(error) if error.contains("-1728") => {
            if let Target::Chat { guid, .. } = target {
                // Messages.app sometimes lists a group chat with the other polarity.
                let flipped = flip_polarity(guid);
                if flipped != *guid {
                    return run_osascript(script, payload, &flipped);
                }
            }
            Err(error)
        }
        result => result,
    }
}

fn flip_polarity(guid: &str) -> String {
    if guid.contains(";+;") {
        guid.replacen(";+;", ";-;", 1)
    } else {
        guid.replacen(";-;", ";+;", 1)
    }
}

/// Runs a fixed AppleScript in an `osascript` child. The payload and address
/// travel as argv, never inside the script, and a child that does not answer
/// in time is killed.
fn run_osascript(script: &str, payload: &str, address: &str) -> Result<(), String> {
    let child = Command::new("/usr/bin/osascript")
        .args(["-", payload, address])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("could not start osascript: {error}"))?;
    write_and_wait(child, script, OSASCRIPT_TIMEOUT)
}

fn write_and_wait(mut child: Child, script: &str, timeout: Duration) -> Result<(), String> {
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(script.as_bytes())
            .map_err(|error| format!("could not hand osascript its script: {error}"))?;
    }
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                if status.success() {
                    return Ok(());
                }
                let detail = stderr.trim();
                return Err(if detail.contains("-1743") {
                    format!("Proxy is not allowed to control Messages: authorize Send Messages in Proxy Settings ({detail})")
                } else {
                    format!("Messages got an error: {detail}")
                });
            }
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "Messages did not answer within {}s; the send outcome is unknown",
                    timeout.as_secs()
                ));
            }
            Ok(None) => std::thread::sleep(POLL_INTERVAL),
            Err(error) => return Err(format!("osascript failed: {error}")),
        }
    }
}

/// Messages.app imports an attachment after the send returns, so the file is
/// copied to a private folder that outlives the call. Copies older than a day
/// are removed on the next send. The bytes come from the handle that
/// [`validate_attachment`] opened with `O_NOFOLLOW`, never from a fresh open of
/// the path, so a symlink or file swapped in after validation is never staged.
fn stage(source: &mut File, path: &Path) -> Result<PathBuf, std::io::Error> {
    let root = std::env::temp_dir();
    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let is_ours = name.to_string_lossy().starts_with(STAGING_PREFIX);
            let expired = entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .map(|modified| modified.elapsed().unwrap_or_default() > STAGING_RETENTION)
                .unwrap_or(false);
            if is_ours && expired {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let directory = root.join(format!("{STAGING_PREFIX}{}-{stamp}", std::process::id()));
    std::fs::create_dir(&directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    }
    let file = directory.join(path.file_name().unwrap_or_default());
    source.seek(SeekFrom::Start(0))?;
    let mut staged = File::create(&file)?;
    std::io::copy(source, &mut staged)?;
    Ok(file)
}

fn latest_message_row_id(conn: &Connection) -> Result<i64, MessagesError> {
    Ok(
        conn.query_row("SELECT COALESCE(MAX(ROWID), 0) FROM message", [], |row| {
            row.get(0)
        })?,
    )
}

/// Reads chat.db until the outgoing rows for this send appear in the target
/// chat, or the confirmation window closes.
fn confirm(
    conn: &Connection,
    target: &Target,
    baseline: i64,
    expected: usize,
) -> Result<Vec<i64>, MessagesError> {
    let deadline = Instant::now() + CONFIRM_TIMEOUT;
    loop {
        let rows = outgoing_rows_since(conn, target, baseline)?;
        if rows.len() >= expected || Instant::now() >= deadline {
            return Ok(rows);
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

pub fn outgoing_rows_since(
    conn: &Connection,
    target: &Target,
    baseline: i64,
) -> Result<Vec<i64>, MessagesError> {
    let (sql, value) = match target {
        Target::Chat { row_id, .. } => (
            "SELECT m.ROWID FROM message m
             JOIN chat_message_join j ON j.message_id = m.ROWID
             WHERE m.is_from_me = 1 AND m.ROWID > ?1 AND j.chat_id = ?2
             ORDER BY m.ROWID",
            rusqlite::types::Value::Integer(*row_id),
        ),
        Target::Participant(handle) => (
            "SELECT m.ROWID FROM message m
             JOIN chat_message_join j ON j.message_id = m.ROWID
             JOIN chat_handle_join chj ON chj.chat_id = j.chat_id
             JOIN handle h ON h.ROWID = chj.handle_id
             WHERE m.is_from_me = 1 AND m.ROWID > ?1 AND LOWER(h.id) = LOWER(?2)
             ORDER BY m.ROWID",
            rusqlite::types::Value::Text(handle.clone()),
        ),
    };
    let mut statement = conn.prepare(sql)?;
    let rows = statement
        .query_map(params![baseline, value], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, chat_identifier TEXT, room_name TEXT, display_name TEXT, service_name TEXT);
            CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT, service TEXT);
            CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
            CREATE TABLE message (ROWID INTEGER PRIMARY KEY, guid TEXT, date INTEGER, text TEXT, is_from_me INTEGER, handle_id INTEGER);
            CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
            INSERT INTO chat VALUES (2, 'any;+;chat85437876530692212', 'chat85437876530692212', 'chat85437876530692212', 'Proxy HV', 'iMessage');
            INSERT INTO chat VALUES (3, 'any;-;+13025353508', '+13025353508', NULL, NULL, 'iMessage');
            INSERT INTO handle VALUES (1, '+13025353508', 'iMessage');
            INSERT INTO chat_handle_join VALUES (3, 1);
            INSERT INTO chat_handle_join VALUES (2, 1);
            "#,
        )
        .unwrap();
        conn
    }

    #[test]
    fn a_group_chat_resolves_from_its_identifier_guid_row_id_or_other_polarity() {
        let conn = fixture();
        let expected = Target::Chat {
            row_id: 2,
            guid: "any;+;chat85437876530692212".into(),
        };
        for spelling in [
            "chat85437876530692212",
            "any;+;chat85437876530692212",
            "any;-;chat85437876530692212",
            "iMessage;+;chat85437876530692212",
            "2",
        ] {
            assert_eq!(
                resolve_target(&conn, spelling).unwrap(),
                expected,
                "{spelling}"
            );
        }
    }

    #[test]
    fn a_phone_number_with_a_direct_chat_resolves_to_that_chat() {
        let conn = fixture();
        assert_eq!(
            resolve_target(&conn, "+13025353508").unwrap(),
            Target::Chat {
                row_id: 3,
                guid: "any;-;+13025353508".into()
            }
        );
        assert_eq!(
            resolve_target(&conn, "someone@example.test").unwrap(),
            Target::Participant("someone@example.test".into())
        );
    }

    #[test]
    fn an_unknown_chat_name_is_an_error_not_a_participant() {
        let conn = fixture();
        let error = resolve_target(&conn, "chat999").unwrap_err().to_string();
        assert!(error.contains("no chat named chat999"), "{error}");
    }

    #[test]
    fn confirmation_sees_only_outgoing_rows_in_the_target_chat_after_the_baseline() {
        let conn = fixture();
        conn.execute_batch(
            "INSERT INTO message VALUES (10, 'old', 0, 'before', 1, NULL);
             INSERT INTO chat_message_join VALUES (2, 10);
             INSERT INTO message VALUES (11, 'theirs', 0, 'inbound', 0, 1);
             INSERT INTO chat_message_join VALUES (2, 11);
             INSERT INTO message VALUES (12, 'ghost', 0, '', 1, NULL);
             INSERT INTO message VALUES (13, 'mine', 0, 'sent', 1, NULL);
             INSERT INTO chat_message_join VALUES (2, 13);",
        )
        .unwrap();
        let target = Target::Chat {
            row_id: 2,
            guid: "any;+;chat85437876530692212".into(),
        };
        assert_eq!(outgoing_rows_since(&conn, &target, 10).unwrap(), vec![13]);
        let direct = Target::Participant("+13025353508".into());
        assert_eq!(outgoing_rows_since(&conn, &direct, 10).unwrap(), vec![13]);
    }

    #[test]
    fn attachments_must_be_media_whose_header_matches_the_extension() {
        let dir = tempfile::tempdir().unwrap();
        let png = dir.path().join("shot.png");
        std::fs::write(&png, [0x89, b'P', b'N', b'G', 0, 0]).unwrap();
        assert_eq!(validate_attachment(&png).unwrap().1, "image/png");
        let fake = dir.path().join("shot.jpg");
        std::fs::write(&fake, b"not a jpeg").unwrap();
        assert!(validate_attachment(&fake).is_err());
        let link = dir.path().join("link.png");
        std::os::unix::fs::symlink(&png, &link).unwrap();
        assert!(validate_attachment(&link).is_err());
        assert!(validate_attachment(Path::new("relative.png")).is_err());

        let ssh_dir = dir.path().join(".ssh");
        std::fs::create_dir(&ssh_dir).unwrap();
        let ssh_png = ssh_dir.join("shot.png");
        std::fs::write(&ssh_png, [0x89, b'P', b'N', b'G', 0, 0]).unwrap();
        let benign_alias = dir.path().join("benign_dir");
        std::os::unix::fs::symlink(&ssh_dir, &benign_alias).unwrap();
        assert!(
            validate_attachment(&benign_alias.join("shot.png")).is_err(),
            "symlink ancestor pointing into .ssh must be refused"
        );
    }

    #[test]
    fn the_staged_bytes_are_the_validated_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let png = dir.path().join("shot.png");
        let body = b"\x89PNG\r\nbody".to_vec();
        std::fs::write(&png, &body).unwrap();
        let (_, _, _, mut validated_file) = validate_attachment(&png).unwrap();

        let secret = dir.path().join("secret.png");
        std::fs::write(&secret, b"SECRET-KEY-MATERIAL-NOT-MEDIA").unwrap();
        std::fs::remove_file(&png).unwrap();
        std::os::unix::fs::symlink(&secret, &png).unwrap();

        let staged = stage(&mut validated_file, &png).unwrap();
        assert_eq!(
            std::fs::read(&staged).unwrap(),
            body,
            "staged bytes must come from the validated handle even after path swap"
        );
        assert!(std::fs::symlink_metadata(&staged).unwrap().is_file());
        assert!(
            open_regular_nofollow(&png).is_err(),
            "O_NOFOLLOW must refuse a swapped symlink"
        );
    }

    #[test]
    fn a_child_that_does_not_answer_is_killed() {
        let child = Command::new("sleep")
            .arg("60")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let started = Instant::now();
        let error = write_and_wait(child, "", Duration::from_millis(200)).unwrap_err();
        assert!(error.contains("did not answer"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn validation_needs_text_or_an_attachment() {
        let error = validate(SendRequest {
            recipient: " ".into(),
            ..Default::default()
        })
        .unwrap_err();
        assert!(error.to_string().contains("recipient"));
        let error = validate(SendRequest {
            recipient: "+1".into(),
            text: Some("  ".into()),
            ..Default::default()
        })
        .unwrap_err();
        assert!(error.to_string().contains("text or attachment_path"));
        let ok = validate(SendRequest {
            recipient: " +1 ".into(),
            text: Some(" hi ".into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            (ok.recipient.as_str(), ok.text.as_deref()),
            ("+1", Some("hi"))
        );
    }
}
