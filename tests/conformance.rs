//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042): it starts, speaks the protocol's version, opens sessions, returns results in the
//! order the contract says, fails without hanging and ends when its input closes. TDengine's REST
//! API cannot stop a statement on the server, so the set's cancel check is skipped.
//!
//! It needs the test server (`ci/services.sh`, `SPELAION_TEST_TDENGINE=127.0.0.1:16041`). Without
//! the variable it is skipped, which does not count as tested.

use spelaion_driver_api::{ConnectParams, Secret};
use spelaion_plugin_host::conformance::{Target, assert_conforms};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-tdengine");

#[tokio::test]
async fn the_plugin_conforms_on_tdengine() {
    let Ok(addr) = std::env::var("SPELAION_TEST_TDENGINE") else {
        eprintln!("SPELAION_TEST_TDENGINE not set: test skipped");
        return;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("a port"),
        user: "root".into(),
        password: Some(Secret::new("taosdata")),
        ..Default::default()
    };
    assert_conforms(&Target {
        program: PROGRAM.into(),
        engine: "tdengine".into(),
        params,
        one_row: "select server_version()".into(),
        refused: "select * from information_schema.a_table_that_is_not_there".into(),
        slow: None,
    })
    .await;
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself. The program says it without a server.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
