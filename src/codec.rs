//! PCM16 @ 8 kHz ↔ Brew traffic frames, through the ETSI EN 300 395-2
//! reference ACELP codec (vendored in `third_party/tetra-codec/`).
//!
//! One Brew `FRAME_TRAFFIC_CHANNEL` payload is the "STE" format every
//! Basestation uses: a header byte (0x00 = normal speech) and the two
//! 137-bit coded subframes of 60 ms of speech packed back-to-back, MSB first
//! (36 bytes).

use std::os::raw::c_int;

pub const PCM_SAMPLES_PER_FRAME: usize = 240;
pub const PCM_SAMPLES_PER_BLOCK: usize = 2 * PCM_SAMPLES_PER_FRAME;
const CODED_BITS: usize = 137;
const CODED_BYTES: usize = 18;
pub const STE_BYTES: usize = 36;
const STE_BITS: usize = 2 * CODED_BITS;

#[allow(non_camel_case_types)]
#[repr(C)]
struct tetra_codec {
    _private: [u8; 0],
}

extern "C" {
    fn tetra_encoder_create() -> *mut tetra_codec;
    fn tetra_decoder_create() -> *mut tetra_codec;
    fn tetra_codec_destroy(st: *mut tetra_codec);
    fn tetra_encode(st: *mut tetra_codec, pcm: *const i16, coded: *mut u8);
    fn tetra_decode(st: *mut tetra_codec, coded: *const u8, pcm: *mut i16, bfi: c_int);
}

struct Handle(*mut tetra_codec);

// Each handle is owned by the single dispatcher task.
unsafe impl Send for Handle {}

impl Drop for Handle {
    fn drop(&mut self) {
        unsafe { tetra_codec_destroy(self.0) };
    }
}

/// Encoder + decoder state for one console. The reference codec is stateful
/// across frames, so `reset` between calls.
pub struct Codec {
    encoder: Handle,
    decoder: Handle,
    ul: Vec<i16>,
}

impl Codec {
    pub fn new() -> Self {
        Self {
            encoder: Handle(unsafe { tetra_encoder_create() }),
            decoder: Handle(unsafe { tetra_decoder_create() }),
            ul: Vec::with_capacity(PCM_SAMPLES_PER_BLOCK * 2),
        }
    }

    /// Fresh codec state for a new call.
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Appends microphone PCM and returns one STE payload per complete 60 ms block.
    pub fn encode_pcm(&mut self, pcm: &[i16]) -> Vec<[u8; STE_BYTES]> {
        self.ul.extend_from_slice(pcm);
        let mut out = Vec::new();
        while self.ul.len() >= PCM_SAMPLES_PER_BLOCK {
            let mut a = [0u8; CODED_BYTES];
            let mut b = [0u8; CODED_BYTES];
            unsafe {
                tetra_encode(self.encoder.0, self.ul[..PCM_SAMPLES_PER_FRAME].as_ptr(), a.as_mut_ptr());
                tetra_encode(self.encoder.0, self.ul[PCM_SAMPLES_PER_FRAME..PCM_SAMPLES_PER_BLOCK].as_ptr(), b.as_mut_ptr());
            }
            self.ul.drain(..PCM_SAMPLES_PER_BLOCK);
            out.push(pack_ste(&a, &b));
        }
        out
    }

    /// Pads a trailing partial block with silence so the last words are not lost.
    pub fn flush(&mut self) -> Vec<[u8; STE_BYTES]> {
        if self.ul.is_empty() {
            return Vec::new();
        }
        let pad = PCM_SAMPLES_PER_BLOCK - self.ul.len();
        self.encode_pcm(&vec![0; pad])
    }

    /// Decodes one Brew traffic payload (36-byte STE, or the bare 35 packed bytes).
    pub fn decode_ste(&mut self, data: &[u8]) -> Option<Vec<i16>> {
        let packed = match data.len() {
            STE_BYTES => &data[1..],
            n if n == STE_BYTES - 1 => data,
            _ => return None,
        };
        let (a, b) = unpack_ste(packed);
        let mut out = Vec::with_capacity(PCM_SAMPLES_PER_BLOCK);
        for frame in [a, b] {
            let mut pcm = [0i16; PCM_SAMPLES_PER_FRAME];
            unsafe { tetra_decode(self.decoder.0, frame.as_ptr(), pcm.as_mut_ptr(), 0) };
            out.extend_from_slice(&pcm);
        }
        Some(out)
    }
}

fn bit(data: &[u8], i: usize) -> u8 {
    (data[i / 8] >> (7 - i % 8)) & 1
}

fn set_bit(data: &mut [u8], i: usize) {
    data[i / 8] |= 1 << (7 - i % 8);
}

fn pack_ste(a: &[u8; CODED_BYTES], b: &[u8; CODED_BYTES]) -> [u8; STE_BYTES] {
    let mut out = [0u8; STE_BYTES];
    let body = &mut out[1..];
    for i in 0..CODED_BITS {
        if bit(a, i) != 0 {
            set_bit(body, i);
        }
        if bit(b, i) != 0 {
            set_bit(body, CODED_BITS + i);
        }
    }
    out
}

fn unpack_ste(packed: &[u8]) -> ([u8; CODED_BYTES], [u8; CODED_BYTES]) {
    let mut a = [0u8; CODED_BYTES];
    let mut b = [0u8; CODED_BYTES];
    for i in 0..STE_BITS {
        if bit(packed, i) != 0 {
            if i < CODED_BITS {
                set_bit(&mut a, i);
            } else {
                set_bit(&mut b, i - CODED_BITS);
            }
        }
    }
    (a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ste_pack_round_trips() {
        let mut a = [0u8; CODED_BYTES];
        let mut b = [0u8; CODED_BYTES];
        for i in 0..CODED_BYTES {
            a[i] = (i as u8).wrapping_mul(37);
            b[i] = (i as u8).wrapping_mul(91) ^ 0x5a;
        }
        // Only 137 bits are meaningful; clear the 7 pad bits.
        a[CODED_BYTES - 1] &= 0x80;
        b[CODED_BYTES - 1] &= 0x80;
        let ste = pack_ste(&a, &b);
        assert_eq!(ste[0], 0);
        assert_eq!(unpack_ste(&ste[1..]), (a, b));
    }

    #[test]
    fn encodes_60ms_blocks_and_decodes_them() {
        let mut c = Codec::new();
        let tone: Vec<i16> = (0..PCM_SAMPLES_PER_BLOCK * 3 + 100)
            .map(|i| ((i as f32 * 0.3).sin() * 8000.0) as i16)
            .collect();
        let blocks = c.encode_pcm(&tone);
        assert_eq!(blocks.len(), 3);
        assert_eq!(c.flush().len(), 1);
        let pcm = c.decode_ste(&blocks[0]).unwrap();
        assert_eq!(pcm.len(), PCM_SAMPLES_PER_BLOCK);
        assert!(c.decode_ste(&[0u8; 10]).is_none());
    }
}
