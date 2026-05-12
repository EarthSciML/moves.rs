//! Round-trip integration tests for the `moves-runspec` TOML and XML
//! formats.
//!
//! What we verify:
//!
//! * **TOML self-loop** — `RunSpec → TOML → RunSpec` is model-identical.
//! * **XML self-loop** — `RunSpec → XML → RunSpec` is model-identical.
//! * **Cross-format** — `XML → RunSpec → TOML → RunSpec → XML → RunSpec`
//!   collapses to a single model value, so the two surfaces really are
//!   isomorphic (Task 13's central promise).
//! * **Fixture sweep** — every fixture in `characterization/fixtures/`
//!   parses, re-serializes, and round-trips through the model.
//! * **Hand-authored fixture parity** — `tests/fixtures/sample-runspec.toml`
//!   loads to the same model as `characterization/fixtures/sample-runspec.xml`.

use std::path::{Path, PathBuf};

use moves_runspec::{from_toml_str, from_xml_str, to_toml_string, to_xml_string};

fn workspace_root() -> PathBuf {
    // tests run with CARGO_MANIFEST_DIR=crates/moves-runspec
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(Path::parent)
        .expect("workspace root above crates/moves-runspec/")
        .to_path_buf()
}

fn fixture(rel: &str) -> PathBuf {
    workspace_root().join(rel)
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[test]
fn xml_self_round_trip_sample() {
    let xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    let spec = from_xml_str(&xml).expect("parse sample-runspec.xml");
    let serialized = to_xml_string(&spec).expect("serialize XML");
    let reparsed = from_xml_str(&serialized).expect("reparse serialized XML");
    assert_eq!(
        spec, reparsed,
        "XML → model → XML → model must be model-identical"
    );
}

#[test]
fn toml_self_round_trip_sample() {
    let toml = read(&fixture(
        "crates/moves-runspec/tests/fixtures/sample-runspec.toml",
    ));
    let spec = from_toml_str(&toml).expect("parse sample-runspec.toml");
    let serialized = to_toml_string(&spec).expect("serialize TOML");
    let reparsed = from_toml_str(&serialized).expect("reparse serialized TOML");
    assert_eq!(
        spec, reparsed,
        "TOML → model → TOML → model must be model-identical"
    );
}

#[test]
fn hand_authored_toml_matches_xml_fixture() {
    let xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    let toml = read(&fixture(
        "crates/moves-runspec/tests/fixtures/sample-runspec.toml",
    ));
    let from_xml = from_xml_str(&xml).expect("parse XML");
    let from_toml = from_toml_str(&toml).expect("parse TOML");
    assert_eq!(
        from_xml, from_toml,
        "the hand-authored TOML and the canonical XML must produce the same model"
    );
}

#[test]
fn cross_format_round_trip_sample() {
    let xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    let from_xml = from_xml_str(&xml).expect("parse XML");
    let toml = to_toml_string(&from_xml).expect("serialize TOML");
    let from_toml = from_toml_str(&toml).expect("parse the serialized TOML");
    let xml2 = to_xml_string(&from_toml).expect("serialize XML");
    let from_xml2 = from_xml_str(&xml2).expect("parse the serialized XML");
    assert_eq!(
        from_xml, from_toml,
        "XML → model → TOML → model must be model-identical"
    );
    assert_eq!(
        from_xml, from_xml2,
        "XML → model → TOML → model → XML → model must collapse to one model"
    );
}

#[test]
fn fixture_sweep_round_trip() {
    let fixtures = std::fs::read_dir(workspace_root().join("characterization/fixtures"))
        .expect("read fixtures dir");
    let mut tested = 0usize;
    for entry in fixtures {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("xml") {
            continue;
        }
        let xml = read(&path);
        let spec = match from_xml_str(&xml) {
            Ok(s) => s,
            Err(e) => panic!("parse {}: {e}", path.display()),
        };

        // XML self-loop
        let xml2 = to_xml_string(&spec).expect("serialize XML");
        let from_xml2 = from_xml_str(&xml2)
            .unwrap_or_else(|e| panic!("reparse serialized XML for {}: {e}", path.display()));
        assert_eq!(
            spec,
            from_xml2,
            "XML self-loop failed for {}",
            path.display()
        );

        // Cross-format
        let toml = to_toml_string(&spec).expect("serialize TOML");
        let from_toml = from_toml_str(&toml)
            .unwrap_or_else(|e| panic!("parse generated TOML for {}: {e}", path.display()));
        assert_eq!(
            spec,
            from_toml,
            "cross-format round-trip failed for {}",
            path.display()
        );
        tested += 1;
    }
    assert!(
        tested >= 10,
        "expected at least 10 fixtures, tested {tested}"
    );
}

#[test]
fn unknown_enum_value_is_an_error() {
    let mut xml = read(&fixture("characterization/fixtures/sample-runspec.xml"));
    xml = xml.replace("MACROSCALE", "NotAScale");
    let err = from_xml_str(&xml).expect_err("invalid modelscale must error");
    let msg = err.to_string();
    assert!(
        msg.contains("modelscale.value") && msg.contains("NotAScale"),
        "error should name the bad field+value: {msg}"
    );
}
