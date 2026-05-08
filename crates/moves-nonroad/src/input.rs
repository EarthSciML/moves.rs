pub mod util;

/// Read and validate the `.opt` options file.
pub fn parse_options(_path: &str) -> super::NonroadOptions {
    // TODO(Task 99): Port opnnon.f / intnon.f option-file processing.
    super::NonroadOptions {
        opt_path: None,
        output_dir: None,
    }
}
