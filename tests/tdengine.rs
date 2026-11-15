//! TDengine through this plugin's own program, as SpelaionDB's core reaches it: every type as a
//! cell (the type zoo), databases, super tables and tables in the explorer, counts of rows,
//! errors, choosing a database, and what connecting refuses.
//!
//! It needs the test server (`ci/services.sh`): `SPELAION_TEST_TDENGINE=127.0.0.1:16041`, user
//! `root`, password `taosdata` (the image's own). Without it it is skipped, which does not count
//! as tested.

use std::time::Duration;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use futures_util::StreamExt;
use spelaion_driver_api::{
    ColumnMeta, ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions,
    LogicalType, ObjectKind, Secret,
};
use spelaion_plugin_host::Plugin;

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-tdengine");

fn params(database: &str) -> Option<ConnectParams> {
    let Ok(addr) = std::env::var("SPELAION_TEST_TDENGINE") else {
        eprintln!("SPELAION_TEST_TDENGINE not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some(ConnectParams {
        host: host.into(),
        port: port.parse().expect("a port"),
        user: "root".into(),
        password: Some(Secret::new("taosdata")),
        database: database.into(),
        connect_timeout: Some(Duration::from_secs(20)),
        ..Default::default()
    })
}

async fn connect(plugin: &Plugin, database: &str) -> Option<Box<dyn Connection>> {
    let params = params(database)?;
    Some(
        plugin
            .driver("tdengine")
            .connect(&params)
            .await
            .expect("connecting"),
    )
}

struct Outcome {
    columns: Vec<ColumnMeta>,
    batches: Vec<arrow::array::RecordBatch>,
    rows_affected: Option<u64>,
}

async fn run(c: &dyn Connection, sql: &str) -> Vec<Result<ExecEvent, DriverError>> {
    let options = ExecOptions {
        batch_size: 2,
        ..Default::default()
    };
    match c.execute(sql, &options).await {
        Ok(events) => tokio::time::timeout(Duration::from_secs(60), events.collect())
            .await
            .expect("the statement ended in time"),
        Err(error) => vec![Err(error)],
    }
}

async fn ok(c: &dyn Connection, sql: &str) -> Outcome {
    let mut out = Outcome {
        columns: Vec::new(),
        batches: Vec::new(),
        rows_affected: None,
    };
    let mut done = false;
    for event in run(c, sql).await {
        match event {
            Ok(ExecEvent::Columns(schema)) => out.columns = schema.columns.clone(),
            Ok(ExecEvent::Batch(batch)) => out.batches.push(batch),
            Ok(ExecEvent::Notice(_)) => {}
            Ok(ExecEvent::Done { rows_affected }) => {
                out.rows_affected = rows_affected;
                done = true;
            }
            Err(error) => panic!("{sql} failed: {error}"),
        }
    }
    assert!(done, "{sql} never ended");
    out
}

impl Outcome {
    fn rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    fn array<T: 'static>(&self, name: &str) -> &T {
        let i = self
            .columns
            .iter()
            .position(|c| c.name == name)
            .unwrap_or_else(|| panic!("no column {name}"));
        self.batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<T>()
            .unwrap()
    }

    fn logical(&self, name: &str) -> LogicalType {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .unwrap()
            .logical
    }
}

async fn fresh_database(c: &dyn Connection, name: &str) {
    ok(c, &format!("drop database if exists {name}")).await;
    ok(c, &format!("create database {name}")).await;
}

#[tokio::test]
async fn every_type_is_a_cell() {
    let plugin = Plugin::new(PROGRAM);
    let Some(c) = connect(&plugin, "").await else {
        return;
    };
    fresh_database(&*c, "spelaion_zoo").await;
    c.select_database("spelaion_zoo").await.unwrap();
    ok(
        &*c,
        "create table zoo (ts timestamp, b bool, ti tinyint, si smallint, i int, bi bigint, \
         ubi bigint unsigned, f float, d double, vc varchar(20), nc nchar(10), vb varbinary(8))",
    )
    .await;
    let inserted = ok(
        &*c,
        "insert into zoo values ('2024-02-29 12:34:56.789', true, -1, 300, 7, \
         9223372036854775807, 18446744073709551614, 1.5, 2.25, 'abc', 'ñandú', '\\x00ff')",
    )
    .await;
    assert_eq!(inserted.rows_affected, Some(1));
    assert!(inserted.columns.is_empty());
    let row = ok(&*c, "select * from zoo").await;
    assert_eq!(row.logical("ts"), LogicalType::TimestampTz);
    assert_eq!(row.logical("ubi"), LogicalType::Decimal);
    assert_eq!(row.logical("vb"), LogicalType::Bytes);
    assert_eq!(
        row.array::<TimestampMicrosecondArray>("ts").value(0),
        1_709_210_096_789_000
    );
    assert!(row.array::<BooleanArray>("b").value(0));
    assert_eq!(row.array::<Int64Array>("ti").value(0), -1);
    assert_eq!(row.array::<Int64Array>("bi").value(0), i64::MAX);
    assert_eq!(
        row.array::<StringArray>("ubi").value(0),
        "18446744073709551614"
    );
    assert_eq!(row.array::<Float64Array>("f").value(0), 1.5);
    assert_eq!(row.array::<StringArray>("nc").value(0), "ñandú");
    assert_eq!(row.array::<BinaryArray>("vb").value(0), [0x00, 0xff]);
    // A row of nulls, past the key.
    ok(&*c, "insert into zoo (ts) values ('2024-03-01 00:00:00')").await;
    let both = ok(&*c, "select vc from zoo order by ts").await;
    assert_eq!(both.rows(), 2);
    assert!(both.batches[0].column(0).is_null(1));
}

#[tokio::test]
async fn databases_super_tables_and_tables_are_what_the_explorer_shows() {
    let plugin = Plugin::new(PROGRAM);
    let Some(c) = connect(&plugin, "").await else {
        return;
    };
    fresh_database(&*c, "spelaion_explorer").await;
    c.select_database("spelaion_explorer").await.unwrap();
    assert_eq!(
        c.current_database().await.unwrap().as_deref(),
        Some("spelaion_explorer")
    );
    ok(
        &*c,
        "create stable meters (ts timestamp, current float, voltage int) \
         tags (location varchar(32), groupid int)",
    )
    .await;
    ok(&*c, "create table d1001 using meters tags ('SF', 2)").await;
    ok(&*c, "create table plain (ts timestamp, v double)").await;
    let schemas = c.list_schemas().await.unwrap();
    assert!(
        schemas
            .iter()
            .any(|s| s.name == "spelaion_explorer" && !s.system)
    );
    assert!(
        schemas
            .iter()
            .any(|s| s.name == "information_schema" && s.system)
    );
    let objects = c.list_objects("spelaion_explorer").await.unwrap();
    let find = |name: &str| objects.iter().find(|o| o.name == name).unwrap();
    assert_eq!(find("meters").kind, ObjectKind::PartitionedTable);
    assert_eq!(find("d1001").kind, ObjectKind::Table);
    assert_eq!(find("d1001").comment.as_deref(), Some("of meters"));
    assert_eq!(find("plain").kind, ObjectKind::Table);
    let meters = c
        .describe_table("spelaion_explorer", "meters")
        .await
        .unwrap();
    assert_eq!(meters.primary_key, ["ts"]);
    let columns: Vec<_> = meters
        .columns
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                c.native_type.as_str(),
                c.comment.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        columns,
        [
            ("ts", "TIMESTAMP", None),
            ("current", "FLOAT", None),
            ("voltage", "INT", None),
            ("location", "VARCHAR(32)", Some("tag")),
            ("groupid", "INT", Some("tag")),
        ]
    );
    let listed = c.list_columns("spelaion_explorer").await.unwrap();
    assert!(
        listed
            .iter()
            .any(|c| c.table == "meters" && c.name == "voltage")
    );
    assert!(listed.iter().all(|c| c.table != "d1001"));
    assert!(c.select_database("no_such_database").await.is_err());
}

#[tokio::test]
async fn an_error_is_the_servers_and_the_session_goes_on() {
    let plugin = Plugin::new(PROGRAM);
    let Some(c) = connect(&plugin, "").await else {
        return;
    };
    let refused = run(&*c, "select * from information_schema.nope").await;
    match refused.into_iter().find_map(Result::err) {
        Some(DriverError::Database { message, code, .. }) => {
            assert!(code.is_some());
            assert!(!message.is_empty());
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(ok(&*c, "select server_version()").await.rows(), 1);
}

#[tokio::test]
async fn connecting_refuses_what_it_cannot_honour() {
    let Some(base) = params("") else {
        return;
    };
    let plugin = Plugin::new(PROGRAM);
    let driver = plugin.driver("tdengine");
    let mut wrong = base.clone();
    wrong.password = Some(Secret::new("not-the-password"));
    let error = driver
        .connect(&wrong)
        .await
        .err()
        .expect("refused")
        .to_string();
    assert!(error.contains("refused the login"), "{error}");
    assert!(!error.contains("not-the-password"), "{error}");
    let mut missing = base.clone();
    missing.database = "no_such_database".into();
    assert!(driver.connect(&missing).await.is_err());
    let mut read_only = base.clone();
    read_only.read_only = true;
    assert!(driver.connect(&read_only).await.is_err());
    let mut options = base;
    options.options.insert("tz".into(), "UTC".into());
    assert!(driver.connect(&options).await.is_err());
}
