use anyhow::{ensure, Result};
use flate2::read::MultiGzDecoder;
use std::io::{Cursor, Read};

const MAX_FIT_BYTES: usize = 12 * 1024 * 1024;
const MAX_ARCHIVE_ENTRIES: usize = 32;

pub fn normalize(input: Vec<u8>) -> Result<Vec<u8>> {
    let bytes = if input.starts_with(b"PK\x03\x04") {
        let mut archive = zip::ZipArchive::new(Cursor::new(input))?;
        ensure!(
            archive.len() <= MAX_ARCHIVE_ENTRIES,
            "too many FIT archive entries"
        );
        let names = archive
            .file_names()
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|name| name.to_ascii_lowercase().ends_with(".fit"))
            .map(|name| name.into_owned())
            .collect::<Vec<_>>();
        ensure!(names.len() == 1, "expected exactly one FIT archive entry");
        let entry = archive.by_name(&names[0])?;
        ensure!(
            entry.size() <= MAX_FIT_BYTES as u64,
            "FIT archive entry exceeds size limit"
        );
        let mut bytes = Vec::new();
        entry
            .take(MAX_FIT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= MAX_FIT_BYTES,
            "FIT archive entry exceeds size limit"
        );
        bytes
    } else if input.starts_with(b"\x1f\x8b") {
        let decoder = MultiGzDecoder::new(Cursor::new(input));
        let mut bytes = Vec::new();
        decoder
            .take(MAX_FIT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= MAX_FIT_BYTES,
            "decompressed FIT exceeds size limit"
        );
        bytes
    } else {
        input
    };
    validate(&bytes)?;
    Ok(bytes)
}

pub fn validate(bytes: &[u8]) -> Result<()> {
    ensure!(
        (14..=MAX_FIT_BYTES).contains(&bytes.len()),
        "invalid FIT size"
    );
    let header = usize::from(bytes[0]);
    ensure!(
        matches!(header, 12 | 14) && &bytes[8..12] == b".FIT",
        "invalid FIT header"
    );
    let data_size = u32::from_le_bytes(bytes[4..8].try_into()?) as usize;
    ensure!(
        data_size > 0 && header + data_size + 2 == bytes.len(),
        "incomplete FIT data"
    );
    let file_crc = u16::from_le_bytes(bytes[bytes.len() - 2..].try_into()?);
    let header_crc = if header == 14 {
        u16::from_le_bytes(bytes[12..14].try_into()?)
    } else {
        0
    };
    ensure!(
        header_crc == 0 || (header == 14 && crc(&bytes[..12]) == header_crc),
        "invalid FIT header checksum"
    );
    let end = bytes.len() - 2;
    ensure!(
        file_crc == crc(&bytes[..end])
            || (header == 14 && header_crc == 0 && crc(&bytes[header..end]) == file_crc),
        "invalid FIT checksum"
    );
    Ok(())
}

fn crc(bytes: &[u8]) -> u16 {
    let mut value = 0u16;
    for byte in bytes {
        value ^= u16::from(*byte);
        for _ in 0..8 {
            value = if value & 1 == 1 {
                (value >> 1) ^ 0xa001
            } else {
                value >> 1
            };
        }
    }
    value
}

#[cfg(test)]
#[path = "fit_file_tests.rs"]
mod tests;
