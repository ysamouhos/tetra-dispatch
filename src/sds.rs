//! SDS Type 4 text messages (ETSI EN 300 392-2 §29, simple / SDS-TL text messaging).

pub const PID_SIMPLE_TEXT: u8 = 0x02;
pub const PID_TEXT: u8 = 0x82;
pub const PID_SIMPLE_IMMEDIATE_TEXT: u8 = 0x09;
pub const PID_IMMEDIATE_TEXT: u8 = 0x89;
/// Text coding scheme: ISO/IEC 8859-1 (Latin 1), no timestamp.
const CODING_LATIN1: u8 = 0x01;
/// Longest text that keeps a Type 4 SDS inside 2047 bits with the SDS-TL header.
pub const MAX_TEXT_CHARS: usize = 250;

/// SDS-TL SDS-TRANSFER with text, no delivery report requested.
pub fn encode_text(text: &str, message_reference: u8) -> Vec<u8> {
    let mut out = vec![PID_TEXT, 0x00, message_reference, CODING_LATIN1];
    out.extend(
        text.chars()
            .take(MAX_TEXT_CHARS)
            .map(|c| if (c as u32) < 0x100 { c as u8 } else { b'?' }),
    );
    out
}

/// SDS-TL header of an incoming Type 4 payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlInfo {
    pub protocol_id: u8,
    pub is_transfer: bool,
    pub report_requested: bool,
    pub message_reference: u8,
}

pub fn tl_info(data: &[u8]) -> Option<TlInfo> {
    let (&pid, rest) = data.split_first()?;
    if pid < 0x80 || rest.len() < 2 {
        return None;
    }
    let is_transfer = rest[0] >> 4 == 0;
    let message_reference = if is_transfer { rest[1] } else { *rest.get(2)? };
    Some(TlInfo {
        protocol_id: pid,
        is_transfer,
        report_requested: is_transfer && (rest[0] >> 2) & 0x03 != 0,
        message_reference,
    })
}

/// SDS-REPORT "received" (`[PID, 0x10, status, MR]`) for a transfer that asked for one.
pub fn build_received_report(info: &TlInfo) -> Vec<u8> {
    vec![info.protocol_id, 0x10, 0x00, info.message_reference]
}

/// Text of a text-messaging SDS, `None` for anything else (reports, LIP, status, ...).
pub fn decode_text(data: &[u8]) -> Option<String> {
    let body: &[u8] = match *data.first()? {
        PID_SIMPLE_TEXT | PID_SIMPLE_IMMEDIATE_TEXT => {
            let coding = *data.get(1)?;
            let start = if coding & 0x80 != 0 { 5 } else { 2 };
            data.get(start..)?
        }
        PID_TEXT | PID_IMMEDIATE_TEXT => {
            let info = tl_info(data)?;
            // Reports carry no text; store/forward control adds a variable-length block.
            if !info.is_transfer || data[1] & 0x01 != 0 {
                return None;
            }
            let coding = *data.get(3)?;
            let start = if coding & 0x80 != 0 { 7 } else { 4 };
            data.get(start..)?
        }
        _ => return None,
    };
    let text: String = body
        .iter()
        .map(|&b| b as char)
        .filter(|c| !c.is_control() || *c == '\n')
        .collect();
    let text = text.trim_end_matches('\0').trim().to_string();
    (!text.is_empty()).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_round_trips() {
        let data = encode_text("Hola, móvil 5", 7);
        assert_eq!(decode_text(&data).as_deref(), Some("Hola, móvil 5"));
        let info = tl_info(&data).unwrap();
        assert!(info.is_transfer && !info.report_requested);
        assert_eq!(info.message_reference, 7);
    }

    #[test]
    fn simple_text_and_reports() {
        assert_eq!(decode_text(&[0x02, 0x01, b'o', b'k']).as_deref(), Some("ok"));
        assert_eq!(decode_text(&[0x82, 0x10, 0x00, 0x03]), None);
        let info = tl_info(&[0x82, 0x04, 0x09, 0x01, b'x']).unwrap();
        assert!(info.report_requested);
        assert_eq!(build_received_report(&info), vec![0x82, 0x10, 0x00, 0x09]);
    }
}
