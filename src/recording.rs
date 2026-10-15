//! Server-side voice recordings: mono PCM16 @ 8 kHz WAV files in one directory.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const SAMPLE_RATE: u32 = 8_000;
const HEADER_LEN: u64 = 44;
const MAX_NAME: usize = 64;

/// Turn a user-typed name into a safe file stem (no path parts, no `.wav`).
pub fn sanitize(name: &str) -> Option<String> {
    let name = name.trim();
    let name = name.strip_suffix(".wav").or_else(|| name.strip_suffix(".WAV")).unwrap_or(name);
    let s: String = name
        .chars()
        .filter_map(|c| match c {
            c if c.is_alphanumeric() || matches!(c, '-' | '_' | '.') => Some(c),
            c if c.is_whitespace() => Some('_'),
            _ => None,
        })
        .take(MAX_NAME)
        .collect();
    let s = s.trim_matches('.').to_string();
    (!s.is_empty()).then_some(s)
}

/// A file name that may be served from the recordings directory.
pub fn valid_file_name(name: &str) -> bool {
    name.ends_with(".wav")
        && !name.starts_with('.')
        && !name.contains(['/', '\\', '\0'])
        && name.len() <= MAX_NAME + 16
}

fn header(data_len: u32) -> [u8; 44] {
    let mut h = [0u8; 44];
    h[0..4].copy_from_slice(b"RIFF");
    h[4..8].copy_from_slice(&(36 + data_len).to_le_bytes());
    h[8..16].copy_from_slice(b"WAVEfmt ");
    h[16..20].copy_from_slice(&16u32.to_le_bytes());
    h[20..22].copy_from_slice(&1u16.to_le_bytes()); // PCM
    h[22..24].copy_from_slice(&1u16.to_le_bytes()); // mono
    h[24..28].copy_from_slice(&SAMPLE_RATE.to_le_bytes());
    h[28..32].copy_from_slice(&(SAMPLE_RATE * 2).to_le_bytes());
    h[32..34].copy_from_slice(&2u16.to_le_bytes());
    h[34..36].copy_from_slice(&16u16.to_le_bytes());
    h[36..40].copy_from_slice(b"data");
    h[40..44].copy_from_slice(&data_len.to_le_bytes());
    h
}

pub struct Recorder {
    out: BufWriter<File>,
    data_len: u32,
    file_name: String,
}

impl Recorder {
    /// Create `<stem>.wav` in `dir` (`<stem>-2.wav`, … if it exists).
    pub fn start(dir: &Path, stem: &str) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        for n in 1..1000 {
            let file_name = if n == 1 { format!("{stem}.wav") } else { format!("{stem}-{n}.wav") };
            match OpenOptions::new().write(true).create_new(true).open(dir.join(&file_name)) {
                Ok(f) => {
                    let mut out = BufWriter::new(f);
                    out.write_all(&header(0))?;
                    return Ok(Self { out, data_len: 0, file_name });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "too many recordings with this name"))
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn write(&mut self, pcm: &[i16]) -> io::Result<()> {
        for s in pcm {
            self.out.write_all(&s.to_le_bytes())?;
        }
        self.data_len = self.data_len.saturating_add(pcm.len() as u32 * 2);
        Ok(())
    }

    /// Patch the sizes into the header and flush.
    pub fn finish(mut self) -> io::Result<String> {
        self.out.flush()?;
        let f = self.out.get_mut();
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&header(self.data_len))?;
        f.flush()?;
        Ok(self.file_name)
    }
}

pub struct Entry {
    pub name: String,
    pub bytes: u64,
    pub modified_ms: u64,
}

impl Entry {
    pub fn seconds(&self) -> u64 {
        self.bytes.saturating_sub(HEADER_LEN) / (SAMPLE_RATE as u64 * 2)
    }
}

/// Recordings in `dir`, newest first.
pub fn list(dir: &Path) -> Vec<Entry> {
    let mut v: Vec<Entry> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            if !valid_file_name(&name) {
                return None;
            }
            let md = e.metadata().ok()?;
            let modified_ms = md.modified().ok()?.duration_since(std::time::UNIX_EPOCH).ok()?.as_millis() as u64;
            Some(Entry { name, bytes: md.len(), modified_ms })
        })
        .collect();
    v.sort_by(|a, b| b.modified_ms.cmp(&a.modified_ms).then(a.name.cmp(&b.name)));
    v
}

pub fn path_of(dir: &Path, name: &str) -> Option<PathBuf> {
    valid_file_name(name).then(|| dir.join(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_names() {
        assert_eq!(sanitize("  Turno noche.wav "), Some("Turno_noche".into()));
        assert_eq!(sanitize("../../etc/passwd"), Some("etcpasswd".into()));
        assert_eq!(sanitize("..."), None);
        assert_eq!(sanitize("   "), None);
    }

    #[test]
    fn only_plain_wav_names_are_served() {
        assert!(valid_file_name("a-1.wav"));
        assert!(!valid_file_name("../a.wav"));
        assert!(!valid_file_name("a/b.wav"));
        assert!(!valid_file_name(".hidden.wav"));
        assert!(!valid_file_name("a.txt"));
    }

    #[test]
    fn writes_valid_wav_and_avoids_overwrite() {
        let dir = std::env::temp_dir().join(format!("td-rec-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut r = Recorder::start(&dir, "x").unwrap();
        r.write(&[1, -1, 2, -2]).unwrap();
        assert_eq!(r.finish().unwrap(), "x.wav");
        let bytes = std::fs::read(dir.join("x.wav")).unwrap();
        assert_eq!(bytes.len(), 44 + 8);
        assert_eq!(&bytes[0..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(bytes[40..44].try_into().unwrap()), 8);
        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 44);
        assert_eq!(Recorder::start(&dir, "x").unwrap().file_name(), "x-2.wav");
        assert_eq!(list(&dir).len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
