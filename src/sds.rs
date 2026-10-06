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

/// Location Information Protocol (ETSI TS 100 392-18-1), plain and over SDS-TL.
pub const PID_LIP: u8 = 0x0A;
pub const PID_LIP_TL: u8 = 0x83;

/// Position from a LIP short or long location report.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Position {
    pub lat: f64,
    pub lon: f64,
    /// km/h, `None` when the radio reports it unknown.
    pub speed: Option<f64>,
    /// Degrees from north, `None` when not reported.
    pub heading: Option<f64>,
}

struct Bits<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Bits<'_> {
    fn take(&mut self, n: usize) -> Option<u32> {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = *self.data.get(self.pos / 8)?;
            v = (v << 1) | ((byte >> (7 - self.pos % 8)) & 1) as u32;
            self.pos += 1;
        }
        Some(v)
    }
}

fn signed(v: u32, bits: u32) -> i32 {
    ((v << (32 - bits)) as i32) >> (32 - bits)
}

/// Horizontal velocity (7 bits) in km/h, `None` when unknown.
fn velocity(v: u32) -> Option<f64> {
    match v {
        0..=28 => Some(v as f64),
        127 => None,
        _ => Some(16.0 * 1.038f64.powi(v as i32 - 13)),
    }
}

fn lon_lat(b: &mut Bits) -> Option<(f64, f64)> {
    let lon = signed(b.take(25)?, 25) as f64 * 360.0 / (1u32 << 25) as f64;
    let lat = signed(b.take(24)?, 24) as f64 * 180.0 / (1u32 << 24) as f64;
    Some((lon, lat))
}

/// Position of a LIP short or long location report, `None` for anything else.
pub fn decode_lip(data: &[u8]) -> Option<Position> {
    let pdu = match *data.first()? {
        PID_LIP => &data[1..],
        PID_LIP_TL => {
            let info = tl_info(data)?;
            if !info.is_transfer || data[1] & 0x01 != 0 {
                return None;
            }
            data.get(3..)?
        }
        _ => return None,
    };
    let mut b = Bits { data: pdu, pos: 0 };
    match b.take(2)? {
        0 => decode_short(&mut b),
        // PDU type extension 3: long location report.
        1 if b.take(4)? == 3 => decode_long(&mut b),
        _ => None,
    }
}

fn decode_short(b: &mut Bits) -> Option<Position> {
    b.take(2)?; // time elapsed
    let (lon, lat) = lon_lat(b)?;
    b.take(3)?; // position error
    let speed = velocity(b.take(7)?);
    let heading = Some(b.take(4)? as f64 * 22.5);
    Some(Position { lat, lon, speed, heading })
}

fn decode_long(b: &mut Bits) -> Option<Position> {
    match b.take(2)? {
        0 => {}
        1 => _ = b.take(2)?,  // time elapsed
        2 => _ = b.take(22)?, // time of position: day, hour, minute, second
        _ => return None,
    }
    let shape = b.take(4)?;
    // Bits each location shape carries after longitude and latitude.
    let extra = match shape {
        1 => 0,  // point
        2 => 6,  // circle: uncertainty
        3 => 22, // ellipse: half axes, angle, confidence
        4 => 12, // point with altitude
        5 => 18, // circle with altitude
        6 => 37, // ellipse with altitude
        7 => 24, // circle with altitude and altitude uncertainty
        8 => 37, // ellipse with altitude and altitude uncertainty
        9 => 51, // arc
        10 => 3, // point with position error
        _ => return None, // no shape, or reserved
    };
    let (lon, lat) = lon_lat(b)?;
    let mut pos = Position { lat, lon, speed: None, heading: None };
    // Velocity is optional detail: a report cut short still gives a position.
    if b.take(extra).is_some() {
        if let Some(vtype) = b.take(3).filter(|&t| t != 0) {
            pos.speed = b.take(7).and_then(velocity);
            // Type 5: horizontal velocity with extended direction of travel.
            if vtype == 5 {
                pos.heading = b.take(8).map(|d| d as f64 * 360.0 / 256.0);
            }
        }
    }
    Some(pos)
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

    #[test]
    fn lip_short_report() {
        // Short report: lon 2.1734 E, lat 41.3851 N, 20 km/h, heading 90°.
        let lon = (2.1734f64 * (1u32 << 25) as f64 / 360.0).round() as u64;
        let lat = (41.3851f64 * (1u32 << 24) as f64 / 180.0).round() as u64;
        let fields = [(0, 2), (0, 2), (lon, 25), (lat, 24), (0, 3), (20, 7), (4, 4), (0, 1), (0, 8)];
        let mut bits = Vec::new();
        for (v, n) in fields {
            bits.extend((0..n).rev().map(|i| (v >> i) & 1 == 1));
        }
        let mut data = vec![PID_LIP];
        data.extend(bits.chunks(8).map(|c| c.iter().enumerate().fold(0u8, |a, (i, &b)| a | (b as u8) << (7 - i))));
        let p = decode_lip(&data).unwrap();
        assert!((p.lon - 2.1734).abs() < 1e-4 && (p.lat - 41.3851).abs() < 1e-4);
        assert_eq!(p.speed, Some(20.0));
        assert_eq!(p.heading, Some(90.0));
        assert_eq!(decode_lip(&[0x82, 0x00, 0x01, 0x01]), None);
    }

    fn pack(fields: &[(u64, usize)]) -> Vec<u8> {
        let mut bits = Vec::new();
        for &(v, n) in fields {
            bits.extend((0..n).rev().map(|i| (v >> i) & 1 == 1));
        }
        let mut data = vec![PID_LIP];
        data.extend(bits.chunks(8).map(|c| c.iter().enumerate().fold(0u8, |a, (i, &b)| a | (b as u8) << (7 - i))));
        data
    }

    #[test]
    fn lip_long_report() {
        let lon = (-3.7038f64 * (1u32 << 25) as f64 / 360.0).round() as i64 as u64 & 0x1FF_FFFF;
        let lat = (40.4168f64 * (1u32 << 24) as f64 / 180.0).round() as u64;
        // Long report, time of position, circle shape, velocity type 5 (20 km/h, 180°).
        let data = pack(&[
            (1, 2), (3, 4), (2, 2), (0, 22), (2, 4), (lon, 25), (lat, 24), (5, 6),
            (5, 3), (20, 7), (128, 8), (0, 1), (0, 8),
        ]);
        let p = decode_lip(&data).unwrap();
        assert!((p.lon + 3.7038).abs() < 1e-4 && (p.lat - 40.4168).abs() < 1e-4);
        assert_eq!(p.speed, Some(20.0));
        assert_eq!(p.heading, Some(180.0));
        // Point shape without velocity data still gives a position.
        let p = decode_lip(&pack(&[(1, 2), (3, 4), (0, 2), (1, 4), (lon, 25), (lat, 24)])).unwrap();
        assert!((p.lat - 40.4168).abs() < 1e-4 && p.speed.is_none() && p.heading.is_none());
    }
}
