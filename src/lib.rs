//! TDengine's driver for SpelaionDB, served as a plugin (SpelaionDB's ADR-041, ADR-056).
//!
//! It runs TDengine SQL, a language shaped like SQL that SpelaionDB's core splits and guards
//! with lists of its own, through taosAdapter's REST API (`POST /rest/sql`). TDengine's own Rust
//! connector is not used: its WebSocket client pulls an unmaintained TLS stack with known
//! advisories. What REST costs, said plainly:
//!
//! - **A result arrives whole** in one answer, and is handed to the core a batch at a time.
//! - **A cancel stops waiting**: the statement the server is running finishes there.
//! - **Each statement is its own request**, run in the session's database (the URL's
//!   `/rest/sql/<database>`); the core refuses `USE`, so the session's database is the one the
//!   tab chose.
//!
//! The core opens every route to the server (tunnels, proxies, Kubernetes): this driver talks only
//! to the address it is given.

mod values;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream;
use serde::Deserialize;
use serde_json::Value;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ColumnInfo, ConnectParams, Connection, Driver, DriverError,
    EventStream, ExecEvent, ExecOptions, KeepAlive, ObjectInfo, ObjectKind, Result, SchemaColumn,
    SchemaInfo, TableInfo, TlsMode,
};
use tokio::sync::{Notify, mpsc};

pub use values::{Kind, Layout, rfc3339_micros};

/// The most child tables of one database the explorer lists: a super table may have one per
/// device, millions of them.
pub const MAX_CHILD_TABLES: usize = 1000;

/// TDengine's own databases.
const SYSTEM_DATABASES: &[&str] = &["information_schema", "performance_schema", "log", "audit"];

/// The `tdengine` engine.
#[derive(Debug, Default, Clone)]
pub struct TdengineDriver;

#[async_trait]
impl Driver for TdengineDriver {
    /// The name SpelaionDB's configuration uses: `engine = "tdengine"`.
    fn engine(&self) -> &str {
        "tdengine"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        if params.read_only {
            return Err(DriverError::Connection(
                "TDengine has no read-only mode in SpelaionDB yet".into(),
            ));
        }
        if let Some(key) = params.options.keys().next() {
            return Err(DriverError::Connection(format!(
                "TDengine has no connection setting `{key}` here"
            )));
        }
        let (host, port) = match params.connect_addr {
            Some(addr) => (addr.ip().to_string(), addr.port()),
            None => (params.host.clone(), params.port),
        };
        let tunnelled = params.connect_addr.is_some();
        let mut client = reqwest::Client::builder()
            .connect_timeout(params.connect_timeout())
            // Nothing the user ran is ever run again (SpelaionDB's AGENTS.md).
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none());
        let scheme = match params.tls {
            // REST has no "encrypt if the server can": `prefer` connects without TLS.
            TlsMode::Disable | TlsMode::Prefer => "http",
            TlsMode::Require => {
                client = client.danger_accept_invalid_certs(true);
                "https"
            }
            TlsMode::VerifyCa | TlsMode::VerifyFull => {
                if tunnelled {
                    return Err(DriverError::Connection(
                        "the certificate is checked against the address this plugin connects \
                         to, which through a tunnel is this machine's: use `require` (encrypted, \
                         the certificate not checked) or connect without a tunnel"
                            .into(),
                    ));
                }
                if let Some(path) = &params.ca_certificate {
                    let pem = std::fs::read(path).map_err(|e| {
                        DriverError::Connection(format!(
                            "the CA certificate {} cannot be read: {e}",
                            path.display()
                        ))
                    })?;
                    for cert in reqwest::Certificate::from_pem_bundle(&pem)
                        .map_err(|e| DriverError::Connection(format!("TLS: {e}")))?
                    {
                        client = client.add_root_certificate(cert);
                    }
                }
                if params.client_certificate.is_some() || params.client_key.is_some() {
                    return Err(DriverError::Connection(
                        "TDengine's REST API logs in with a user and password, not a client \
                         certificate"
                            .into(),
                    ));
                }
                "https"
            }
        };
        let client = client
            .build()
            .map_err(|e| DriverError::Connection(format!("TLS: {e}")))?;
        let user = if params.user.is_empty() {
            "root".to_string()
        } else {
            params.user.clone()
        };
        let shared = Arc::new(Shared {
            client,
            base: format!("{scheme}://{host}:{port}/rest/sql"),
            user,
            password: params
                .password
                .as_ref()
                .map(|p| p.expose().to_string())
                .unwrap_or_default(),
            database: Mutex::new(params.database.clone()),
        });
        // A round trip says now whether the server answers and the login works.
        let timeout = params.connect_timeout();
        match tokio::time::timeout(timeout, shared.sql("select server_version()", None)).await {
            Ok(Ok(_)) => {}
            Ok(Err(DriverError::Database { message, .. })) => {
                return Err(DriverError::Connection(message));
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(DriverError::Connection(format!(
                    "the server did not answer within {} s",
                    timeout.as_secs()
                )));
            }
        }
        if !params.database.is_empty() {
            // The database must exist: a request in one that does not fails later otherwise.
            let databases = shared
                .texts("select name from information_schema.ins_databases")
                .await?;
            if !databases.iter().any(|d| d == &params.database) {
                return Err(DriverError::Connection(format!(
                    "there is no database `{}`",
                    params.database
                )));
            }
        }
        Ok(Box::new(TdengineConnection {
            shared,
            capabilities: Capabilities {
                cancel: true,
                schemas: true,
                ..Capabilities::default()
            },
            running: Arc::new(Mutex::new(None)),
        }))
    }
}

/// REST's answer.
#[derive(Debug, Deserialize)]
struct Answer {
    code: i64,
    #[serde(default)]
    desc: Option<String>,
    #[serde(default)]
    column_meta: Vec<(String, String, Value)>,
    #[serde(default)]
    data: Vec<Vec<Value>>,
}

struct Shared {
    client: reqwest::Client,
    base: String,
    user: String,
    password: String,
    database: Mutex<String>,
}

impl Shared {
    fn database(&self) -> String {
        self.database.lock().expect("lock poisoned").clone()
    }

    /// One statement, in `database` (the session's when `None`), as REST answers it. Times come
    /// back in UTC.
    async fn sql(&self, sql: &str, database: Option<&str>) -> Result<Answer> {
        let database = database
            .map(str::to_string)
            .unwrap_or_else(|| self.database());
        let url = if database.is_empty() {
            format!("{}?tz=UTC", self.base)
        } else {
            format!("{}/{}?tz=UTC", self.base, percent(&database))
        };
        let response = self
            .client
            .post(url)
            .basic_auth(&self.user, Some(&self.password))
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| DriverError::Connection(http_error(&e)))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| DriverError::Connection(http_error(&e)))?;
        let answer: Answer = serde_json::from_slice(&body).map_err(|_| {
            DriverError::Connection(format!(
                "the server answered {status} with something that is not TDengine's REST API"
            ))
        })?;
        if answer.code != 0 {
            let message = answer
                .desc
                .clone()
                .unwrap_or_else(|| format!("error {}", answer.code));
            if status == reqwest::StatusCode::UNAUTHORIZED
                || message.contains("Authentication failure")
                || message.contains("Invalid user")
            {
                return Err(DriverError::Connection(format!(
                    "the server refused the login: {message}"
                )));
            }
            return Err(DriverError::Database {
                code: Some(format!("{:#06x}", answer.code)),
                message,
                detail: None,
                hint: None,
                position: None,
            });
        }
        Ok(answer)
    }

    /// The first column of a catalog query's rows, as text.
    async fn texts(&self, sql: &str) -> Result<Vec<String>> {
        Ok(self
            .sql(sql, Some(""))
            .await?
            .data
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect())
    }
}

/// An error of the HTTP client with its causes. The URL it names holds no secret: the password
/// goes in the `Authorization` header, never in the address.
fn http_error(error: &reqwest::Error) -> String {
    let mut text = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(inner) = source {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        source = inner.source();
    }
    text
}

/// A database name in the URL's path.
fn percent(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A name in a catalog query's string literal.
fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

struct TdengineConnection {
    shared: Arc<Shared>,
    capabilities: Capabilities,
    running: Arc<Mutex<Option<Arc<Notify>>>>,
}

/// Whether REST's answer is a statement's count of rows, not rows: one `affected_rows` column.
fn affected_rows(answer: &Answer) -> Option<u64> {
    match answer.column_meta.as_slice() {
        [(name, _, _)] if name == "affected_rows" => answer
            .data
            .first()
            .and_then(|row| row.first())
            .and_then(Value::as_u64),
        _ => None,
    }
}

#[async_trait]
impl Connection for TdengineConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        let cancelled = Arc::new(Notify::new());
        *self.running.lock().expect("lock poisoned") = Some(cancelled.clone());
        let shared = self.shared.clone();
        let sql = sql.to_string();
        let batch_size = opts.batch_size.max(1);
        let (sender, receiver) = mpsc::channel::<Result<ExecEvent>>(1);
        tokio::spawn(async move {
            let answer = tokio::select! {
                biased;
                () = cancelled.notified() => Err(DriverError::Cancelled),
                answer = shared.sql(&sql, None) => answer,
            };
            let answer = match answer {
                Ok(answer) => answer,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            // A statement that changes rows answers with their count, not with rows.
            let first_word = sql
                .trim_start()
                .split(|c: char| !c.is_ascii_alphabetic())
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            let reads = matches!(
                first_word.as_str(),
                "select" | "show" | "describe" | "desc" | "explain"
            );
            if !reads && let Some(count) = affected_rows(&answer) {
                let _ = sender
                    .send(Ok(ExecEvent::Done {
                        rows_affected: Some(count),
                    }))
                    .await;
                return;
            }
            let meta: Vec<(String, String)> = answer
                .column_meta
                .iter()
                .map(|(name, native, _)| (name.clone(), native.clone()))
                .collect();
            let layout = Layout::of(&meta);
            if sender
                .send(Ok(ExecEvent::Columns(layout.schema.clone())))
                .await
                .is_err()
            {
                return;
            }
            for rows in answer.data.chunks(batch_size) {
                let batch = layout.batch(rows);
                let failed = batch.is_err();
                if sender.send(batch.map(ExecEvent::Batch)).await.is_err() || failed {
                    return;
                }
            }
            let _ = sender
                .send(Ok(ExecEvent::Done {
                    rows_affected: None,
                }))
                .await;
        });
        Ok(Box::pin(stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|event| (event, receiver)) },
        )))
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(Cancel(self.running.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        Ok(self
            .shared
            .texts("select server_version()")
            .await?
            .into_iter()
            .next()
            .unwrap_or_default())
    }

    async fn keep_alive(&self) -> Result<KeepAlive> {
        self.shared.sql("select server_version()", Some("")).await?;
        Ok(KeepAlive::Alive)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        let database = self.shared.database();
        Ok((!database.is_empty()).then_some(database))
    }

    async fn database_context(&self) -> Result<Option<String>> {
        self.current_database().await
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        let databases = self
            .shared
            .texts("select name from information_schema.ins_databases")
            .await?;
        if !databases.iter().any(|d| d == database) {
            return Err(DriverError::Other(format!(
                "there is no database `{database}`"
            )));
        }
        *self.shared.database.lock().expect("lock poisoned") = database.to_string();
        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(self
            .list_schemas()
            .await?
            .into_iter()
            .map(|s| s.name)
            .collect())
    }

    /// A TDengine database is what the explorer shows as a schema.
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let mut schemas: Vec<SchemaInfo> = self
            .shared
            .texts("select name from information_schema.ins_databases")
            .await?
            .into_iter()
            .map(|name| SchemaInfo {
                system: SYSTEM_DATABASES.contains(&name.as_str()),
                name,
            })
            .collect();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(schemas)
    }

    /// Super tables (shown as partitioned tables: each is the parent of its child tables),
    /// normal tables, and child tables up to [`MAX_CHILD_TABLES`], each with its super table as
    /// its comment.
    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let db = literal(schema);
        let mut objects = Vec::new();
        let stables = self
            .shared
            .sql(
                &format!(
                    "select stable_name, table_comment from information_schema.ins_stables \
                     where db_name = {db}"
                ),
                Some(""),
            )
            .await?;
        for row in stables.data {
            if let Some(name) = row.first().and_then(Value::as_str) {
                objects.push(ObjectInfo {
                    schema: schema.to_string(),
                    name: name.to_string(),
                    kind: ObjectKind::PartitionedTable,
                    estimated_rows: None,
                    comment: row
                        .get(1)
                        .and_then(Value::as_str)
                        .filter(|c| !c.is_empty())
                        .map(str::to_string),
                });
            }
        }
        let tables = self
            .shared
            .sql(
                &format!(
                    "select table_name, stable_name, table_comment from information_schema.ins_tables \
                     where db_name = {db} order by stable_name, table_name limit {}",
                    MAX_CHILD_TABLES + 10_000
                ),
                Some(""),
            )
            .await?;
        let mut children = 0;
        for row in tables.data {
            let Some(name) = row.first().and_then(Value::as_str) else {
                continue;
            };
            let parent = row.get(1).and_then(Value::as_str);
            if parent.is_some() {
                children += 1;
                if children > MAX_CHILD_TABLES {
                    continue;
                }
            }
            objects.push(ObjectInfo {
                schema: schema.to_string(),
                name: name.to_string(),
                kind: ObjectKind::Table,
                estimated_rows: None,
                comment: match parent {
                    Some(parent) => Some(format!("of {parent}")),
                    None => row
                        .get(2)
                        .and_then(Value::as_str)
                        .filter(|c| !c.is_empty())
                        .map(str::to_string),
                },
            });
        }
        objects.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(objects)
    }

    /// Its columns and tags (`DESCRIBE`), the first timestamp column as the key (TDengine keys
    /// every row by it, within a table).
    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let object = self
            .list_objects(schema)
            .await?
            .into_iter()
            .find(|o| o.name == name)
            .ok_or_else(|| DriverError::Other(format!("there is no table {schema}.{name}")))?;
        let quote = |s: &str| format!("`{}`", s.replace('`', "``"));
        let described = self
            .shared
            .sql(
                &format!("describe {}.{}", quote(schema), quote(name)),
                Some(""),
            )
            .await?;
        let mut columns = Vec::new();
        let mut primary_key = Vec::new();
        for row in described.data {
            let text = |i: usize| row.get(i).and_then(Value::as_str).unwrap_or("").to_string();
            let (field, native, note) = (text(0), text(1), text(3));
            let length = row.get(2).and_then(Value::as_i64).unwrap_or(0);
            let native = if matches!(
                native.as_str(),
                "VARCHAR" | "NCHAR" | "BINARY" | "VARBINARY" | "GEOMETRY"
            ) {
                format!("{native}({length})")
            } else {
                native
            };
            if primary_key.is_empty() && native == "TIMESTAMP" && note.is_empty() {
                primary_key.push(field.clone());
            }
            columns.push(ColumnInfo {
                name: field,
                native_type: native,
                nullable: primary_key.last().is_none_or(|key| key != &text(0)),
                default: None,
                comment: (note == "TAG").then(|| "tag".to_string()),
            });
        }
        Ok(TableInfo {
            object,
            columns,
            primary_key,
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            constraints: Vec::new(),
        })
    }

    /// The columns of the database's super tables and normal tables (child tables have their
    /// super table's).
    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        let answer = self
            .shared
            .sql(
                &format!(
                    "select table_name, col_name, col_type from information_schema.ins_columns \
                     where db_name = {} and table_type in ('SUPER_TABLE', 'NORMAL_TABLE')",
                    literal(schema)
                ),
                Some(""),
            )
            .await?;
        Ok(answer
            .data
            .into_iter()
            .filter_map(|row| {
                let text = |i: usize| row.get(i).and_then(Value::as_str).map(str::to_string);
                Some(SchemaColumn {
                    table: text(0)?,
                    name: text(1)?,
                    native_type: text(2)?,
                })
            })
            .collect())
    }
}

struct Cancel(Arc<Mutex<Option<Arc<Notify>>>>);

#[async_trait]
impl CancelHandle for Cancel {
    async fn cancel(&self) -> Result<()> {
        if let Some(running) = self.0.lock().expect("lock poisoned").as_ref() {
            running.notify_one();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_quoted_where_they_go() {
        assert_eq!(percent("demo"), "demo");
        assert_eq!(percent("a b/c"), "a%20b%2Fc");
        assert_eq!(literal("it's"), r"'it\'s'");
    }

    #[test]
    fn a_count_of_rows_is_told_from_rows() {
        let answer: Answer = serde_json::from_str(
            r#"{"code":0,"column_meta":[["affected_rows","INT",4]],"data":[[3]],"rows":1}"#,
        )
        .unwrap();
        assert_eq!(affected_rows(&answer), Some(3));
        let rows: Answer = serde_json::from_str(
            r#"{"code":0,"column_meta":[["a","INT",4],["b","INT",4]],"data":[[1,2]],"rows":1}"#,
        )
        .unwrap();
        assert_eq!(affected_rows(&rows), None);
    }
}
