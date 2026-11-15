# spelaiondb-plugin-tdengine

TDengine for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), served as a plugin: a
program SpelaionDB starts and talks to over its standard input and output (its ADR-041).

SpelaionDB's core splits and guards TDengine SQL with lists of its own (its ADR-056) and sends
each statement as it is written; the plugin runs it through taosAdapter's REST API and returns its
rows. The explorer shows databases, super tables, normal tables and up to 1,000 child tables per
database, with their columns and tags. Read-only mode and agents are not offered on TDengine yet.

- **Tested against:** TDengine 3.3.6 (`testing/compose.yaml`).
- **Through REST, so:** a result arrives whole and is handed over a batch at a time, and a cancel
  stops waiting while the statement finishes on the server. TDengine's own Rust connector is not
  used: its WebSocket client pulls an unmaintained TLS stack with known advisories.
- **Not tested here:** TDengine Cloud (a paid service with token logins).

Protocol: [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
version 1.1, experimental until SpelaionDB 1.0. Rules for contributors and agents: `AGENTS.md`.
License: Apache-2.0.
