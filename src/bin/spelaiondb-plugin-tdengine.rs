//! The plugin's program: this crate's driver, served to SpelaionDB over its plugin protocol on
//! the program's standard input and output (SpelaionDB's ADR-041).
//!
//! It does nothing else. Standard output is the protocol: nothing here, and nothing in the
//! driver, ever prints to it.

use std::sync::Arc;

use spelaion_driver_tdengine::TdengineDriver;
use spelaion_plugin_sdk::{Plugin, run};

#[tokio::main]
async fn main() {
    run(Plugin::new(
        "spelaiondb-plugin-tdengine",
        env!("CARGO_PKG_VERSION"),
        vec![Arc::new(TdengineDriver)],
    ))
    .await
}
