//! `moves convert-runspec` — convert a RunSpec between XML and TOML.
//!
//! Both directions route through the canonical [`moves_runspec::RunSpec`]
//! model, so XML↔TOML conversion is lossless by construction (migration-plan
//! Task 13). The conversion direction is inferred from file extensions: the
//! input's extension picks the parser, the output's extension picks the
//! serializer. With no `--output`, the target format is the opposite of the
//! input's and the output path is the input path with its extension swapped.
//!
//! Same-format conversions (e.g. `.xml` → `.xml`) are allowed and act as a
//! normaliser: the result is the canonical serialization of the model.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use moves_runspec::{to_toml_string, to_xml_string};

use crate::{load_run_spec, RunSpecFormat};

/// Inputs for one `moves convert-runspec` invocation.
#[derive(Debug, Clone)]
pub struct ConvertOptions {
    /// RunSpec to convert. Its extension selects the input parser.
    pub input: PathBuf,
    /// Destination path. `None` derives it from the input path with the
    /// target format's extension.
    pub output: Option<PathBuf>,
}

/// What [`convert_runspec`] did — the resolved paths and formats.
#[derive(Debug, Clone)]
pub struct ConvertOutcome {
    /// The input path that was read.
    pub input: PathBuf,
    /// The output path that was written.
    pub output: PathBuf,
    /// The format the input was parsed as.
    pub from: RunSpecFormat,
    /// The format the output was written as.
    pub to: RunSpecFormat,
}

/// Convert a RunSpec between XML and TOML.
///
/// # Errors
///
/// Fails if either path has an unrecognised extension, the input cannot be
/// read or parsed, or the output cannot be serialized or written.
pub fn convert_runspec(opts: &ConvertOptions) -> Result<ConvertOutcome> {
    let from = RunSpecFormat::from_path(&opts.input).with_context(|| {
        format!(
            "cannot infer input format from {}: expected a .xml, .mrs, or .toml extension",
            opts.input.display()
        )
    })?;
    let (to, output) = match &opts.output {
        Some(path) => {
            let to = RunSpecFormat::from_path(path).with_context(|| {
                format!(
                    "cannot infer output format from {}: expected a .xml, .mrs, or .toml extension",
                    path.display()
                )
            })?;
            (to, path.clone())
        }
        None => {
            let to = from.opposite();
            (to, opts.input.with_extension(to.extension()))
        }
    };

    let run_spec = load_run_spec(&opts.input)?;
    let rendered = match to {
        RunSpecFormat::Xml => to_xml_string(&run_spec),
        RunSpecFormat::Toml => to_toml_string(&run_spec),
    }
    .with_context(|| format!("serializing RunSpec to {}", to.label()))?;
    fs::write(&output, rendered).with_context(|| format!("writing {}", output.display()))?;

    Ok(ConvertOutcome {
        input: opts.input.clone(),
        output,
        from,
        to,
    })
}
