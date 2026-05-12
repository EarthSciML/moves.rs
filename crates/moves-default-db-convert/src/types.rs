//! MySQL → Arrow type mapping for the default-DB conversion pipeline.
//!
//! The mapping matches `moves-fixture-capture::tabular::mysql_type_to_kind`
//! semantically (int family → Int64, float family → Float64, bool/boolean →
//! Boolean, everything else → Utf8) but produces an Arrow [`DataType`]
//! directly. Kept in sync with that module if either mapping changes.
//!
//! Width caveat: MariaDB's `INFORMATION_SCHEMA.COLUMNS.DATA_TYPE` reports
//! `tinyint` without the `(1)` display width that distinguishes a flag from a
//! signed byte. We widen all integer flavors to Int64 — losslessly correct
//! and aligned with the snapshot crate so cross-stage joins line up.

use arrow::datatypes::DataType;

/// Map a `INFORMATION_SCHEMA.COLUMNS.DATA_TYPE` value to an Arrow [`DataType`].
pub fn mysql_to_arrow(mysql_type: &str) -> DataType {
    let t = mysql_type.trim().to_ascii_lowercase();
    match t.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            DataType::Int64
        }
        "decimal" | "numeric" | "float" | "double" | "real" => DataType::Float64,
        "bool" | "boolean" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

/// Strip parameterized MySQL type declarations, e.g. `int(11) unsigned` →
/// `int`. Some legacy dumps include the display width.
pub fn normalize_mysql_type(decl: &str) -> String {
    let lowered = decl.trim().to_ascii_lowercase();
    let head = match lowered.find('(') {
        Some(i) => &lowered[..i],
        None => &lowered,
    };
    head.trim_end_matches(" unsigned")
        .trim_end_matches(" zerofill")
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_family_maps_to_int64() {
        for t in [
            "tinyint",
            "smallint",
            "mediumint",
            "int",
            "integer",
            "bigint",
            "year",
        ] {
            assert_eq!(mysql_to_arrow(t), DataType::Int64, "{t}");
        }
    }

    #[test]
    fn float_family_maps_to_float64() {
        for t in ["decimal", "numeric", "float", "double", "real"] {
            assert_eq!(mysql_to_arrow(t), DataType::Float64, "{t}");
        }
    }

    #[test]
    fn bool_maps_to_boolean() {
        assert_eq!(mysql_to_arrow("bool"), DataType::Boolean);
        assert_eq!(mysql_to_arrow("BOOLEAN"), DataType::Boolean);
    }

    #[test]
    fn string_dates_blobs_map_to_utf8() {
        for t in [
            "varchar", "char", "text", "longtext", "blob", "date", "datetime", "time", "enum",
        ] {
            assert_eq!(mysql_to_arrow(t), DataType::Utf8, "{t}");
        }
    }

    #[test]
    fn whitespace_and_case_tolerant() {
        assert_eq!(mysql_to_arrow("  INT "), DataType::Int64);
        assert_eq!(mysql_to_arrow("DOUBLE"), DataType::Float64);
    }

    #[test]
    fn normalize_strips_width_and_modifiers() {
        assert_eq!(normalize_mysql_type("int(11)"), "int");
        assert_eq!(normalize_mysql_type("int(11) unsigned"), "int");
        assert_eq!(normalize_mysql_type("DOUBLE"), "double");
        assert_eq!(normalize_mysql_type("varchar(255)"), "varchar");
        assert_eq!(normalize_mysql_type(" smallint(6) zerofill "), "smallint");
    }
}
