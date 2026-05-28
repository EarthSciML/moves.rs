//! Parquet I/O for Polars DataFrames stored in a [`DataFrameStore`].
//!
//! Provides [`DataFrameStoreParquet`], a blanket extension trait that adds
//! [`write_parquet`](DataFrameStoreParquet::write_parquet) and
//! [`read_parquet`](DataFrameStoreParquet::read_parquet) to every
//! [`DataFrameStore`] implementation.
//!
//! # Writer settings
//!
//! Settings mirror the determinism contract from `output_processor.rs`:
//!
//! * `UNCOMPRESSED`
//! * statistics off ([`StatisticsOptions::empty`])
//! * fixed `created_by = "Polars"` (embedded by `polars-parquet`)
//!
//! With identical row contents these settings produce byte-identical Parquet,
//! which the [`crate::data`] round-trip determinism contract depends on.

use std::io::Write;

use polars::io::mmap::MmapBytesReader;
use polars::prelude::{
    DataFrame, ParquetCompression, ParquetReader, ParquetWriter, SerReader, StatisticsOptions,
};

use super::DataFrameStore;
use crate::error::{Error, Result};

/// Extension trait adding Parquet I/O to any [`DataFrameStore`].
///
/// Blanket-implemented for every [`DataFrameStore`] implementation; call-sites
/// need `use crate::data::DataFrameStoreParquet` (or the re-exported path
/// from `crate::data`).
pub trait DataFrameStoreParquet: DataFrameStore {
    /// Serialize the DataFrame stored under `name` to `dest` as Parquet.
    ///
    /// The writer is configured for byte-deterministic output: UNCOMPRESSED,
    /// statistics off. Two calls with the same [`DataFrame`] always produce
    /// byte-identical files.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Polars`] when `name` is absent from the store or when
    /// the Parquet encoder fails.
    fn write_parquet<W: Write>(&self, name: &str, dest: W) -> Result<()> {
        let arc_df = self
            .get(name)
            .ok_or_else(|| Error::Polars(format!("table '{name}' not found in store")))?;
        let mut df = arc_df.as_ref().clone();
        ParquetWriter::new(dest)
            .with_compression(ParquetCompression::Uncompressed)
            .with_statistics(StatisticsOptions::empty())
            .finish(&mut df)
            .map_err(|e| Error::Polars(e.to_string()))?;
        Ok(())
    }

    /// Read Parquet data from `src` and insert the resulting [`DataFrame`]
    /// into this store under `name`.
    ///
    /// `src` can be any [`MmapBytesReader`] — a `BufReader<File>` for
    /// streaming file reads or a `Cursor<&[u8]>` for in-memory bytes.
    /// Prefer the file-backed form so raw bytes are released as they decode
    /// rather than holding a full copy alongside the decoded buffers.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Polars`] when `src` is not valid Parquet or when
    /// type inference fails.
    fn read_parquet(&mut self, name: &str, src: impl MmapBytesReader) -> Result<()> {
        let df = ParquetReader::new(src)
            .finish()
            .map_err(|e| Error::Polars(e.to_string()))?;
        self.insert(name, df);
        Ok(())
    }
}

impl<S: DataFrameStore> DataFrameStoreParquet for S {}

/// Write `df` to `dest` as Parquet using the deterministic writer settings.
///
/// Exposed as a standalone helper for callers that hold a [`DataFrame`]
/// directly and do not need a [`DataFrameStore`]. The [`DataFrameStoreParquet`]
/// blanket impl delegates to this function.
pub fn write_dataframe(df: &DataFrame, dest: impl Write) -> Result<()> {
    let mut df = df.clone();
    ParquetWriter::new(dest)
        .with_compression(ParquetCompression::Uncompressed)
        .with_statistics(StatisticsOptions::empty())
        .finish(&mut df)
        .map_err(|e| Error::Polars(e.to_string()))?;
    Ok(())
}

/// Read Parquet data into a [`DataFrame`].
///
/// Exposed as a standalone helper for callers that do not need to insert the
/// result into a [`DataFrameStore`] immediately.
pub fn read_dataframe(src: impl MmapBytesReader) -> Result<DataFrame> {
    ParquetReader::new(src)
        .finish()
        .map_err(|e| Error::Polars(e.to_string()))
}
