//! `parse_backend_payload` only produces `Command`/`BatchCommands` values whose
//! `request` field is a plain `String`. These tests show the different ways an
//! application can convert that raw request string into the type it actually needs.

use std::str::FromStr;

use ascii_tcp::{parse_backend_payload, Payload};
use regex::Regex;

/// Parses `json` and returns the `request` field of the single command it describes.
fn single_request(json: &str) -> String {
    match parse_backend_payload(json).unwrap() {
        Payload::Single(command) => command.request,
        Payload::Batch(_) => panic!("expected a single command payload"),
    }
}

/// Builds a minimal single-command backend payload with the given `request` text.
fn payload_json(request: &str) -> String {
    format!(
        r#"{{
            "id": "value",
            "request": "{request}",
            "protocol": {{
                "type": "ascii-tcp",
                "host": "127.0.0.1",
                "port": 4000,
                "line_ending": "lf"
            }}
        }}"#
    )
}

#[test]
fn parses_request_into_double() {
    let request = single_request(&payload_json("21.5"));
    let value: f64 = request.parse().unwrap();
    assert_eq!(value, 21.5);
}

#[test]
fn parses_request_into_long() {
    let request = single_request(&payload_json("-4200000000"));
    let value: i64 = request.parse().unwrap();
    assert_eq!(value, -4_200_000_000);
}

#[test]
fn parses_request_into_ulong() {
    let request = single_request(&payload_json("18446744073709"));
    let value: u64 = request.parse().unwrap();
    assert_eq!(value, 18_446_744_073_709);
}

#[test]
fn parses_request_into_string() {
    // The request is already a `String`, so no conversion is needed.
    let value = single_request(&payload_json("READ TEMP"));
    assert_eq!(value, "READ TEMP");
}

#[test]
fn parses_request_into_char() {
    let request = single_request(&payload_json("A"));
    let value: char = request.parse().unwrap();
    assert_eq!(value, 'A');
}

#[test]
fn parses_request_into_byte() {
    let request = single_request(&payload_json("255"));
    let value: u8 = request.parse().unwrap();
    assert_eq!(value, 255);
}

/// Application-defined enum that maps request text (e.g. a device's alarm state) onto
/// a Rust type via `FromStr`, so it can be parsed the same way as the numeric types above.
#[derive(Debug, PartialEq, Eq)]
enum AlarmLevel {
    Ok,
    Warning,
    Critical,
}

impl FromStr for AlarmLevel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "OK" => Ok(Self::Ok),
            "WARNING" => Ok(Self::Warning),
            "CRITICAL" => Ok(Self::Critical),
            other => Err(format!("unknown alarm level: {other}")),
        }
    }
}

#[test]
fn parses_request_into_enum() {
    let request = single_request(&payload_json("CRITICAL"));
    let value: AlarmLevel = request.parse().unwrap();
    assert_eq!(value, AlarmLevel::Critical);
}

/// Devices often reply with a text prefix before the value, e.g. `"TEMP 32.3"` for a
/// `"TEMP?"` request. A regex pulls out just the numeric part so it can be parsed.
#[test]
fn extracts_double_from_a_prefixed_response() {
    let response = "TEMP 32.3";
    let pattern = Regex::new(r"(-?\d+(?:\.\d+)?)").unwrap();
    let captured = pattern
        .captures(response)
        .and_then(|captures| captures.get(1))
        .expect("response did not contain a numeric value");

    let value: f64 = captured.as_str().parse().unwrap();
    assert_eq!(value, 32.3);
}
