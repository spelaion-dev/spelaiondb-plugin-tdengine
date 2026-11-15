//! TDengine's REST answers as Arrow columns, typed from the column's TDengine type
//! (SpelaionDB's ADR-005): `TIMESTAMP` in UTC (taosAdapter writes RFC 3339 with the `tz=UTC` it is
//! asked for), `BOOL`, every integer width as integers except a `BIGINT UNSIGNED` past `i64`
//! (exact text), `FLOAT` and `DOUBLE`, `VARCHAR`, `BINARY` and `NCHAR` as text, `VARBINARY` and
//! `GEOMETRY` as bytes (taosAdapter sends them as hex), `JSON` tags as JSON, and `DECIMAL` as its
//! exact text. Nothing is rounded or dropped.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder,
};
use serde_json::Value;
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// What a column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Int,
    /// `BIGINT UNSIGNED`: an integer while it fits, its exact text otherwise.
    UnsignedBig,
    Float,
    Bool,
    Text,
    Bytes,
    Timestamp,
    Json,
    Decimal,
}

impl Kind {
    /// The kind of a column TDengine names `native` (`INT`, `VARCHAR`, `BIGINT UNSIGNED`…).
    pub fn of(native: &str) -> Kind {
        let upper = native.to_ascii_uppercase();
        let base = upper.split('(').next().unwrap_or("").trim();
        match base {
            "BIGINT UNSIGNED" => Kind::UnsignedBig,
            "TINYINT" | "SMALLINT" | "INT" | "BIGINT" | "TINYINT UNSIGNED"
            | "SMALLINT UNSIGNED" | "INT UNSIGNED" => Kind::Int,
            "FLOAT" | "DOUBLE" => Kind::Float,
            "BOOL" => Kind::Bool,
            "TIMESTAMP" => Kind::Timestamp,
            "VARBINARY" | "GEOMETRY" => Kind::Bytes,
            "JSON" => Kind::Json,
            "DECIMAL" => Kind::Decimal,
            _ => Kind::Text,
        }
    }

    fn logical(self) -> LogicalType {
        match self {
            Kind::Int => LogicalType::Int,
            // Its values past `i64` would not fit an integer column: all exact text.
            Kind::UnsignedBig | Kind::Decimal => LogicalType::Decimal,
            Kind::Float => LogicalType::Float,
            Kind::Bool => LogicalType::Bool,
            Kind::Text => LogicalType::Text,
            Kind::Bytes => LogicalType::Bytes,
            Kind::Timestamp => LogicalType::TimestampTz,
            Kind::Json => LogicalType::Json,
        }
    }
}

/// A result's columns: their names, kinds and TDengine types.
#[derive(Debug, Clone)]
pub struct Layout {
    pub kinds: Vec<Kind>,
    pub schema: Arc<ResultSchema>,
}

impl Layout {
    /// The layout of REST's `column_meta`: `[[name, type, length], …]`.
    pub fn of(meta: &[(String, String)]) -> Layout {
        let kinds: Vec<Kind> = meta.iter().map(|(_, native)| Kind::of(native)).collect();
        let columns = meta
            .iter()
            .zip(&kinds)
            .map(|((name, native), kind)| ColumnMeta {
                name: name.clone(),
                logical: kind.logical(),
                native_type: native.to_ascii_uppercase(),
                native_id: None,
                origin: None,
            })
            .collect();
        Layout {
            kinds,
            schema: Arc::new(ResultSchema::new(columns)),
        }
    }

    /// `rows` (REST's `data`, one JSON array per row) as one batch.
    pub fn batch(&self, rows: &[Vec<Value>]) -> Result<RecordBatch> {
        let n = rows.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.kinds.len());
        for (i, kind) in self.kinds.iter().enumerate() {
            let cells = rows.iter().map(|row| row.get(i).unwrap_or(&Value::Null));
            let array: ArrayRef = match kind {
                Kind::Int => {
                    let mut b = Int64Builder::with_capacity(n);
                    for cell in cells {
                        b.append_option(cell.as_i64());
                    }
                    Arc::new(b.finish())
                }
                Kind::Float => {
                    let mut b = Float64Builder::with_capacity(n);
                    for cell in cells {
                        b.append_option(cell.as_f64());
                    }
                    Arc::new(b.finish())
                }
                Kind::Bool => {
                    let mut b = BooleanBuilder::with_capacity(n);
                    for cell in cells {
                        b.append_option(cell.as_bool());
                    }
                    Arc::new(b.finish())
                }
                Kind::Timestamp => {
                    let mut b = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
                    for cell in cells {
                        b.append_option(cell.as_str().and_then(rfc3339_micros));
                    }
                    Arc::new(b.finish())
                }
                Kind::Bytes => {
                    let mut b = BinaryBuilder::with_capacity(n, n * 16);
                    for cell in cells {
                        match cell.as_str().and_then(unhex) {
                            Some(bytes) => b.append_value(bytes),
                            None => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                Kind::Json => {
                    let mut b = StringBuilder::with_capacity(n, n * 16);
                    for cell in cells {
                        match cell {
                            Value::Null => b.append_null(),
                            // A JSON tag arrives as its text, or already parsed.
                            Value::String(text) => b.append_value(text),
                            other => b.append_value(other.to_string()),
                        }
                    }
                    Arc::new(b.finish())
                }
                Kind::UnsignedBig | Kind::Decimal | Kind::Text => {
                    let mut b = StringBuilder::with_capacity(n, n * 16);
                    for cell in cells {
                        match cell {
                            Value::Null => b.append_null(),
                            Value::String(text) => b.append_value(text),
                            other => b.append_value(other.to_string()),
                        }
                    }
                    Arc::new(b.finish())
                }
            };
            arrays.push(array);
        }
        RecordBatch::try_new(self.schema.arrow.clone(), arrays)
            .map_err(|e| DriverError::Other(format!("building a batch of rows: {e}")))
    }
}

/// `2024-02-29T12:34:56.789Z` (or with an offset) as microseconds since 1970 in UTC. Digits
/// past the microsecond (a database of nanosecond precision) are dropped toward zero.
pub fn rfc3339_micros(text: &str) -> Option<i64> {
    let b = text.as_bytes();
    let num = |from: usize, len: usize| -> Option<i64> {
        std::str::from_utf8(b.get(from..from + len)?)
            .ok()?
            .parse()
            .ok()
    };
    let negative = b.first() == Some(&b'-');
    let start = usize::from(negative);
    let year = num(start, 4)? * if negative { -1 } else { 1 };
    let (month, day) = (num(start + 5, 2)?, num(start + 8, 2)?);
    let (hour, minute, second) = (
        num(start + 11, 2)?,
        num(start + 14, 2)?,
        num(start + 17, 2)?,
    );
    let mut i = start + 19;
    let mut micros = 0i64;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let mut digits = 0;
        while let Some(c) = b.get(i).filter(|c| c.is_ascii_digit()) {
            if digits < 6 {
                micros = micros * 10 + i64::from(c - b'0');
                digits += 1;
            }
            i += 1;
        }
        for _ in digits..6 {
            micros *= 10;
        }
    }
    let offset_seconds = match b.get(i) {
        Some(b'Z') | None => 0,
        Some(sign @ (b'+' | b'-')) => {
            let (h, m) = (num(i + 1, 2)?, num(i + 4, 2)?);
            let seconds = h * 3600 + m * 60;
            if *sign == b'+' { seconds } else { -seconds }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3600 + minute * 60 + second - offset_seconds;
    seconds.checked_mul(1_000_000)?.checked_add(micros)
}

/// Days since 1970-01-01 of a date of the proleptic Gregorian calendar (Howard Hinnant's).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    let text = text.strip_prefix("\\x").unwrap_or(text);
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn timestamps_are_microseconds_in_utc() {
        assert_eq!(rfc3339_micros("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            rfc3339_micros("2024-02-29T12:34:56.789Z"),
            Some(1_709_210_096_789_000)
        );
        assert_eq!(
            rfc3339_micros("2024-02-29T12:34:56.789123456Z"),
            Some(1_709_210_096_789_123)
        );
        assert_eq!(
            rfc3339_micros("2024-02-29T13:34:56.789+01:00"),
            Some(1_709_210_096_789_000)
        );
        assert_eq!(rfc3339_micros("1969-12-31T23:59:59Z"), Some(-1_000_000));
        assert_eq!(rfc3339_micros("not a time"), None);
    }

    #[test]
    fn each_type_keeps_its_value() {
        let layout = Layout::of(&[
            ("ubi".into(), "BIGINT UNSIGNED".into()),
            ("vb".into(), "VARBINARY".into()),
            ("vc".into(), "VARCHAR".into()),
            ("i".into(), "INT".into()),
        ]);
        assert_eq!(layout.schema.columns[0].logical, LogicalType::Decimal);
        let batch = layout
            .batch(&[vec![
                json!(18446744073709551614u64),
                json!("00ff"),
                json!("ñandú"),
                json!(7),
            ]])
            .unwrap();
        let text = |c: usize| {
            batch
                .column(c)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(text(0), "18446744073709551614");
        assert_eq!(text(2), "ñandú");
        let bytes = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(bytes.value(0), [0x00, 0xff]);
    }
}
