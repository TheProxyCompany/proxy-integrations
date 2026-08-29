# Proxy Integrations

Native, reusable integrations for local services exposed by Proxy.

The first module is `messages`, a read-only macOS Messages database client. It
opens `~/Library/Messages/chat.db` in SQLite read-only mode, honors its live WAL,
and exposes stable chat, message, reaction, and attachment records. Contact
authorization and sending remain host responsibilities so consumers can use
their own signed macOS application and capability policy.

Reading Messages history requires Full Disk Access for the host process. The
`check_access` API reports that state without mutating Messages or its database.

```rust
use proxy_integrations::messages::{MessageQuery, MessagesStore};

let store = MessagesStore::default_for_current_user()?;
let messages = store.messages(MessageQuery::default())?;
# Ok::<(), proxy_integrations::messages::MessagesError>(())
```

The crate is MIT licensed. See `NOTICE` for attribution of the typedstream
message-body parser.
