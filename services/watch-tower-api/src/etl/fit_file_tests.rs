use super::*;
use anyhow::Result;
use flate2::{write::GzEncoder, Compression};
use std::io::Write;

fn fixture() -> Vec<u8> {
    let mut bytes = vec![12, 0x10, 0, 0];
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(b".FIT");
    bytes.push(0);
    let file_crc = crc(&bytes);
    bytes.extend_from_slice(&file_crc.to_le_bytes());
    bytes
}

#[test]
fn validates_fit_header_and_both_checksums() -> Result<()> {
    let bytes = fixture();
    validate(&bytes)?;
    let mut corrupt = bytes;
    corrupt[12] ^= 1;
    assert!(validate(&corrupt).is_err());
    Ok(())
}

#[test]
fn normalizes_plain_fit_bytes() -> Result<()> {
    let bytes = fixture();
    assert_eq!(normalize(bytes.clone())?, bytes);
    Ok(())
}

#[test]
fn normalizes_gzip_wrapped_fit_bytes() -> Result<()> {
    let expected = fixture();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&expected)?;
    assert_eq!(normalize(encoder.finish()?)?, expected);
    Ok(())
}

#[test]
fn normalizes_single_fit_entry_from_zip_archive() -> Result<()> {
    let expected = fixture();
    let cursor = Cursor::new(Vec::new());
    let mut archive = zip::ZipWriter::new(cursor);
    archive.start_file("workout.fit", zip::write::SimpleFileOptions::default())?;
    archive.write_all(&expected)?;
    let archive = archive.finish()?.into_inner();
    assert_eq!(normalize(archive)?, expected);
    Ok(())
}
