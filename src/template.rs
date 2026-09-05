//! printf-style response templates.
//!
//! A template describes the full layout of a response line and marks the
//! interesting fields with conversion specifiers:
//!
//! | Specifier    | Matches                                       |
//! |--------------|-----------------------------------------------|
//! | `%d` `%i` `%u` | optionally signed decimal integer           |
//! | `%f` `%e` `%g` | decimal number with optional fraction/exponent |
//! | `%x` `%X`    | hexadecimal integer, optional `0x` prefix     |
//! | `%s`         | free text up to the next literal              |
//! | `%%`         | a literal `%`                                 |
//!
//! Prefixing a conversion with `*` matches the field but discards it, as in
//! `scanf`. `"VEL,GPIO6=%*d,GPIO2=%*d,DELTA=%d"` captures only `DELTA`.
//!
//! ```
//! use ascii_tcp::template::parse_response_template;
//!
//! let captures = parse_response_template("ARM=%d", "ARM=1").unwrap();
//! assert_eq!(captures[0].as_i64(), Some(1));
//! ```
//!
//! Matching is anchored: the whole response (after trimming surrounding
//! whitespace) must be consumed, so a malformed reply is reported as a
//! [`AsciiTcpError::ResponseMismatch`] instead of silently yielding a value.

use crate::AsciiTcpError;

/// The value type a conversion specifier extracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecKind {
    Int,
    Float,
    Hex,
    Text,
}

#[derive(Debug, Clone)]
enum Segment {
    Literal(String),
    Spec { kind: SpecKind, capture: bool },
}

/// One field extracted from a response by a [`ResponseTemplate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capture {
    pub kind: SpecKind,
    pub text: String,
}

impl Capture {
    /// The matched text exactly as it appeared in the response.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self.kind {
            SpecKind::Hex => {
                let (negative, digits) = split_sign(&self.text);
                let digits = digits
                    .strip_prefix("0x")
                    .or_else(|| digits.strip_prefix("0X"))
                    .unwrap_or(digits);
                let magnitude = i64::from_str_radix(digits, 16).ok()?;
                Some(if negative { -magnitude } else { magnitude })
            }
            _ => self
                .text
                .parse::<i64>()
                .ok()
                .or_else(|| self.text.parse::<f64>().ok().map(|v| v as i64)),
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self.kind {
            SpecKind::Hex => self.as_i64().map(|v| v as f64),
            _ => self.text.parse::<f64>().ok(),
        }
    }
}

/// A compiled response template.
///
/// Compile once and reuse it for every poll to avoid re-parsing the pattern.
#[derive(Debug, Clone)]
pub struct ResponseTemplate {
    segments: Vec<Segment>,
    source: String,
}

impl ResponseTemplate {
    /// Compiles a template, rejecting unsupported or ambiguous patterns.
    pub fn compile(template: &str) -> Result<Self, AsciiTcpError> {
        let mut segments: Vec<Segment> = Vec::new();
        let mut literal = String::new();
        let mut chars = template.chars();

        while let Some(c) = chars.next() {
            if c != '%' {
                literal.push(c);
                continue;
            }
            match chars.next() {
                Some('%') => literal.push('%'),
                Some(first) => {
                    let capture = first != '*';
                    let spec = if capture {
                        first
                    } else {
                        chars.next().ok_or_else(|| {
                            AsciiTcpError::TemplateSyntax(format!(
                                "template '{template}' ends with an incomplete '%*' conversion"
                            ))
                        })?
                    };
                    let kind = match spec {
                        'd' | 'i' | 'u' => SpecKind::Int,
                        'f' | 'e' | 'g' => SpecKind::Float,
                        'x' | 'X' => SpecKind::Hex,
                        's' => SpecKind::Text,
                        other => {
                            return Err(AsciiTcpError::TemplateSyntax(format!(
                                "unsupported conversion '%{other}' in template '{template}'"
                            )))
                        }
                    };
                    if !literal.is_empty() {
                        segments.push(Segment::Literal(std::mem::take(&mut literal)));
                    } else if matches!(segments.last(), Some(Segment::Spec { .. })) {
                        return Err(AsciiTcpError::TemplateSyntax(format!(
                            "template '{template}' has adjacent conversions with no separator between them"
                        )));
                    }
                    segments.push(Segment::Spec { kind, capture });
                }
                None => {
                    return Err(AsciiTcpError::TemplateSyntax(format!(
                        "template '{template}' ends with a trailing '%'"
                    )))
                }
            }
        }
        if !literal.is_empty() {
            segments.push(Segment::Literal(literal));
        }

        Ok(Self {
            segments,
            source: template.to_string(),
        })
    }

    /// The template string this was compiled from.
    pub fn as_str(&self) -> &str {
        &self.source
    }

    /// Number of conversion specifiers that produce a capture.
    pub fn spec_count(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| matches!(s, Segment::Spec { capture: true, .. }))
            .count()
    }

    /// Matches `response` against the template and returns every captured field.
    pub fn captures(&self, response: &str) -> Result<Vec<Capture>, AsciiTcpError> {
        let text = response.trim();
        let mut pos = 0usize;
        let mut captures = Vec::with_capacity(self.spec_count());

        for (index, segment) in self.segments.iter().enumerate() {
            match segment {
                Segment::Literal(literal) => {
                    if !text[pos..].starts_with(literal.as_str()) {
                        return Err(self.mismatch(response));
                    }
                    pos += literal.len();
                }
                Segment::Spec { kind, capture } => {
                    let next_literal = match self.segments.get(index + 1) {
                        Some(Segment::Literal(literal)) => Some(literal.as_str()),
                        _ => None,
                    };
                    let len = match kind {
                        SpecKind::Text => match next_literal {
                            Some(literal) => text[pos..]
                                .find(literal)
                                .ok_or_else(|| self.mismatch(response))?,
                            None => text.len() - pos,
                        },
                        _ => {
                            pos += leading_whitespace(&text[pos..]);
                            scan_number(&text[pos..], *kind)
                        }
                    };
                    if len == 0 {
                        return Err(self.mismatch(response));
                    }
                    if *capture {
                        captures.push(Capture {
                            kind: *kind,
                            text: text[pos..pos + len].to_string(),
                        });
                    }
                    pos += len;
                }
            }
        }

        if pos != text.len() {
            return Err(self.mismatch(response));
        }
        Ok(captures)
    }

    /// Matches `response` and returns the first captured field.
    pub fn capture_first(&self, response: &str) -> Result<Capture, AsciiTcpError> {
        self.captures(response)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                AsciiTcpError::TemplateSyntax(format!(
                    "template '{}' has no conversion specifier to capture",
                    self.source
                ))
            })
    }

    fn mismatch(&self, response: &str) -> AsciiTcpError {
        AsciiTcpError::ResponseMismatch {
            template: self.source.clone(),
            response: response.to_string(),
        }
    }
}

/// Compiles `template` and matches it against `response` in one step.
pub fn parse_response_template(
    template: &str,
    response: &str,
) -> Result<Vec<Capture>, AsciiTcpError> {
    ResponseTemplate::compile(template)?.captures(response)
}

/// Compiles `template` and returns only the first captured field.
pub fn parse_first_value(template: &str, response: &str) -> Result<Capture, AsciiTcpError> {
    ResponseTemplate::compile(template)?.capture_first(response)
}

fn split_sign(text: &str) -> (bool, &str) {
    match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    }
}

fn leading_whitespace(text: &str) -> usize {
    text.len() - text.trim_start_matches([' ', '\t']).len()
}

/// Returns the byte length of the numeric prefix of `text`, or 0 if there is none.
fn scan_number(text: &str, kind: SpecKind) -> usize {
    let bytes = text.as_bytes();
    let mut i = 0;

    if matches!(bytes.first(), Some(b'-') | Some(b'+')) {
        i += 1;
    }

    if kind == SpecKind::Hex {
        if text[i..].starts_with("0x") || text[i..].starts_with("0X") {
            i += 2;
        }
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
            i += 1;
        }
        return if i == start { 0 } else { i };
    }

    let int_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    let mut digits = i - int_start;

    if kind == SpecKind::Float {
        if bytes.get(i) == Some(&b'.') {
            i += 1;
            let frac_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            digits += i - frac_start;
        }
        if digits > 0 && matches!(bytes.get(i), Some(b'e') | Some(b'E')) {
            let mut j = i + 1;
            if matches!(bytes.get(j), Some(b'-') | Some(b'+')) {
                j += 1;
            }
            let exp_start = j;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > exp_start {
                i = j;
            }
        }
    }

    if digits == 0 {
        0
    } else {
        i
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_arm_query_response() {
        for (response, expected) in [("ARM=0", 0), ("ARM=1", 1)] {
            let capture = parse_first_value("ARM=%d", response).unwrap();
            assert_eq!(capture.as_i64(), Some(expected));
            assert_eq!(capture.as_f64(), Some(expected as f64));
        }
    }

    #[test]
    fn trims_surrounding_whitespace_and_skips_padding() {
        let capture = parse_first_value("ARM=%d", "  ARM=  1 \t").unwrap();
        assert_eq!(capture.as_str(), "1");
    }

    #[test]
    fn rejects_response_that_does_not_match() {
        let error = parse_first_value("ARM=%d", "FIRE=1").unwrap_err();
        assert!(matches!(error, AsciiTcpError::ResponseMismatch { .. }));

        let error = parse_first_value("ARM=%d", "ARM=ON").unwrap_err();
        assert!(matches!(error, AsciiTcpError::ResponseMismatch { .. }));

        let error = parse_first_value("ARM=%d", "ARM=1 STALE").unwrap_err();
        assert!(matches!(error, AsciiTcpError::ResponseMismatch { .. }));
    }

    #[test]
    fn parses_signed_floats_and_exponents() {
        assert_eq!(
            parse_first_value("P=%f psi", "P=-12.5 psi").unwrap().as_f64(),
            Some(-12.5)
        );
        assert_eq!(
            parse_first_value("P=%f", "P=1.25e-3").unwrap().as_f64(),
            Some(1.25e-3)
        );
    }

    #[test]
    fn parses_hex_with_and_without_prefix() {
        assert_eq!(
            parse_first_value("STATUS=%x", "STATUS=0x1F").unwrap().as_i64(),
            Some(31)
        );
        assert_eq!(
            parse_first_value("STATUS=%X", "STATUS=1f").unwrap().as_i64(),
            Some(31)
        );
    }

    #[test]
    fn captures_multiple_fields_and_text() {
        let captures = parse_response_template("%s=%d,%f", "ARM=1,2.5").unwrap();
        assert_eq!(captures[0].as_str(), "ARM");
        assert_eq!(captures[1].as_i64(), Some(1));
        assert_eq!(captures[2].as_f64(), Some(2.5));
    }

    #[test]
    fn escaped_percent_is_a_literal() {
        let capture = parse_first_value("DUTY=%d%%", "DUTY=75%").unwrap();
        assert_eq!(capture.as_i64(), Some(75));
    }

    #[test]
    fn suppressed_conversions_are_matched_but_not_captured() {
        let template = "VEL,GPIO6=%*d,GPIO2=%*d,DELTA=%d(ticks at 8ns)";
        let capture =
            parse_first_value(template, "VEL,GPIO6=1,GPIO2=0,DELTA=12345(ticks at 8ns)").unwrap();
        assert_eq!(capture.as_i64(), Some(12345));
        assert_eq!(
            ResponseTemplate::compile(template).unwrap().spec_count(),
            1
        );
    }

    #[test]
    fn rejects_malformed_templates() {
        assert!(matches!(
            ResponseTemplate::compile("ARM=%q").unwrap_err(),
            AsciiTcpError::TemplateSyntax(_)
        ));
        assert!(matches!(
            ResponseTemplate::compile("ARM=%").unwrap_err(),
            AsciiTcpError::TemplateSyntax(_)
        ));
        assert!(matches!(
            ResponseTemplate::compile("%d%d").unwrap_err(),
            AsciiTcpError::TemplateSyntax(_)
        ));
        assert!(matches!(
            ResponseTemplate::compile("ARM=%*").unwrap_err(),
            AsciiTcpError::TemplateSyntax(_)
        ));
    }

    #[test]
    fn template_without_specifier_matches_but_captures_nothing() {
        let template = ResponseTemplate::compile("OK").unwrap();
        assert_eq!(template.captures("OK").unwrap().len(), 0);
        assert!(template.capture_first("OK").is_err());
        assert!(template.captures("NAK").is_err());
    }
}
