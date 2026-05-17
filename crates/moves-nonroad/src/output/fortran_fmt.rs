//! Fortran output edit-descriptor formatting.
//!
//! NONROAD's output writers (`wrt*.f`, `hdrbmy.f`) build their
//! records with Fortran `WRITE` statements driven by `FORMAT`
//! statements. Reproducing the legacy NONROAD text output
//! byte-for-byte (Task 114's backwards-compatibility requirement)
//! means reproducing the four edit descriptors those formats use:
//!
//! | Descriptor | Helper | Used by |
//! |---|---|---|
//! | `Ew.d` | [`fortran_e`] | `wrtdat`, `wrtbmy`, `wrtams`, `wrtsi` emission/value columns |
//! | `Fw.d` | [`fortran_f`] | `wrthdr`'s leading dummy record (`F3.0`) |
//! | `Iw`   | [`fortran_i`] | HP, model year, FIPS-derived integers, AMS filler |
//! | `Aw`   | [`fortran_a`] | every character column |
//!
//! This module is the shared primitive layer; it ports no single
//! Fortran file but the `WRITE`/`FORMAT` machinery the Task 114
//! writers depend on.
//!
//! # `Ew.d` form
//!
//! Fortran writes a real with `Ew.d` as a normalised mantissa
//! `0.1 ≤ |m| < 1.0` carrying `d` digits, then `E`, then a signed
//! two-digit exponent: `s0.dddddddd E±ee`. The leading `0` is
//! processor-dependent and appears only when the field is wide
//! enough — `w ≥ d + 7` keeps it (`E15.8`: `w = 15 = 8 + 7`),
//! `w = d + 6` drops it (`E10.4`, `E12.6`), and a narrower field
//! overflows to asterisks. NONROAD's `f32` (`real*4`) values never
//! reach a three-digit exponent, so the two-digit exponent field is
//! always sufficient.
//!
//! # `Aw` form
//!
//! Every `Aw` (and width-less `A`) edit in the NONROAD writers acts
//! on a `character` variable whose declared width is at least `w`,
//! so `Aw` reduces to "left-justify the value in `w`, pad with
//! blanks, truncate to the leftmost `w` if longer". The general
//! Fortran rule's right-justifying `w > len` branch never arises
//! with non-blank data here and is not reproduced.

/// Format a real with the Fortran `Ew.d` edit descriptor.
///
/// `width` is the total field width `w`; `decimals` is the mantissa
/// digit count `d`. The value is taken as `real*4` (the writers all
/// carry `f32` emissions and quantities) but the decimal expansion
/// runs in `f64` so the `d`-digit rounding is exact.
///
/// Returns a string of exactly `width` characters: the formatted
/// value right-justified, or `width` asterisks if it does not fit
/// (`width < d + 6`).
pub fn fortran_e(value: f32, width: usize, decimals: usize) -> String {
    let d = decimals;
    let negative = value < 0.0;
    let magnitude = f64::from(value).abs();

    // Decompose into a `d`-digit integer mantissa and an exponent so
    // that `value ≈ ± 0.<digits> × 10^exp`.
    let (digits, exp): (i64, i32) = if magnitude == 0.0 || !magnitude.is_finite() {
        // Zero prints as `0.0…0E+00`. A non-finite value cannot occur
        // in a valid emissions record; format it as zero defensively
        // rather than panicking inside a writer.
        (0, 0)
    } else {
        // Normalise the mantissa to `0.1 ≤ m < 1.0`. `log10` can land
        // a hair either side of an exact power of ten, so the result
        // is corrected by one ulp of exponent if needed.
        let mut exp = magnitude.log10().floor() as i32 + 1;
        let mut mantissa = magnitude / 10f64.powi(exp);
        if mantissa >= 1.0 {
            exp += 1;
            mantissa = magnitude / 10f64.powi(exp);
        } else if mantissa < 0.1 {
            exp -= 1;
            mantissa = magnitude / 10f64.powi(exp);
        }
        // Round the mantissa to `d` digits; a round-up that reaches
        // 10^d carries into the exponent.
        let pow_d = 10i64.pow(d as u32);
        let mut digits = (mantissa * pow_d as f64).round() as i64;
        if digits >= pow_d {
            digits /= 10;
            exp += 1;
        }
        (digits, exp)
    };

    // The leading `0` survives only in a field wide enough for it.
    let leading = if width >= d + 7 { "0." } else { "." };
    let exp_sign = if exp < 0 { '-' } else { '+' };
    let core = format!(
        "{leading}{digits:0d$}E{exp_sign}{exp_abs:02}",
        exp_abs = exp.unsigned_abs(),
    );
    let full = if negative { format!("-{core}") } else { core };

    if full.len() > width {
        "*".repeat(width)
    } else {
        format!("{full:>width$}")
    }
}

/// Format a real with the Fortran `Fw.d` edit descriptor.
///
/// `decimals` may be zero, in which case the decimal point is still
/// emitted with no fractional digits (`F3.0` of `0.0` → `" 0."`).
/// Returns `width` asterisks if the value does not fit.
pub fn fortran_f(value: f32, width: usize, decimals: usize) -> String {
    let negative = value < 0.0;
    let magnitude = f64::from(value).abs();
    let scaled = (magnitude * 10f64.powi(decimals as i32)).round() as i128;

    let body = if decimals == 0 {
        format!("{scaled}.")
    } else {
        // Zero-pad so there is at least one digit before the point,
        // then splice the decimal point `decimals` places from the
        // right.
        let mut digits = format!("{scaled:0min$}", min = decimals + 1);
        digits.insert(digits.len() - decimals, '.');
        digits
    };
    let full = if negative { format!("-{body}") } else { body };

    if full.len() > width {
        "*".repeat(width)
    } else {
        format!("{full:>width$}")
    }
}

/// Format an integer with the Fortran `Iw` edit descriptor.
///
/// Right-justifies the value (with its sign, if negative) in `width`
/// columns, or returns `width` asterisks when it does not fit.
pub fn fortran_i(value: i64, width: usize) -> String {
    let body = value.to_string();
    if body.len() > width {
        "*".repeat(width)
    } else {
        format!("{body:>width$}")
    }
}

/// Format a string with the Fortran `Aw` edit descriptor.
///
/// Left-justifies `value` in `width` columns, padding with trailing
/// blanks, or truncates to the leftmost `width` characters when
/// `value` is longer. See the module documentation for why the
/// general rule's right-justifying branch is omitted.
pub fn fortran_a(value: &str, width: usize) -> String {
    if value.chars().count() >= width {
        value.chars().take(width).collect()
    } else {
        format!("{value:<width$}")
    }
}

/// A column-positioned output line, for the Fortran `T` (tab) and `X`
/// (skip) edit descriptors.
///
/// The message-file writers (`wrtmsg.f`, `wrtsum.f`) lay their lines
/// out by column with `Tn` and `nX` rather than by concatenation.
/// `FortranLine` reproduces that: it keeps a write cursor and a byte
/// buffer, [`tab`](FortranLine::tab)s the cursor to an absolute
/// column or [`skip`](FortranLine::skip)s it forward, and
/// [`text`](FortranLine::text) writes at the cursor — extending the
/// buffer with blanks across a gap, or overwriting when a `Tn`
/// tabbed the cursor back into already-written content (which
/// `wrtsum.f`'s national-record line does deliberately).
#[derive(Debug, Default, Clone)]
pub struct FortranLine {
    buf: Vec<u8>,
    cursor: usize,
}

impl FortranLine {
    /// An empty line with the cursor at column 1.
    pub fn new() -> Self {
        Self::default()
    }

    /// Move the cursor to 1-based `column` — the Fortran `Tn`
    /// descriptor. `column` 0 is treated as column 1.
    pub fn tab(&mut self, column: usize) {
        self.cursor = column.saturating_sub(1);
    }

    /// Move the cursor forward `n` columns — the Fortran `nX`
    /// descriptor.
    pub fn skip(&mut self, n: usize) {
        self.cursor += n;
    }

    /// Write `text` at the cursor, advancing it. A gap between the
    /// buffer end and the cursor is filled with blanks; content the
    /// cursor has been tabbed back over is overwritten.
    pub fn text(&mut self, text: &str) {
        for &byte in text.as_bytes() {
            if self.cursor >= self.buf.len() {
                self.buf.resize(self.cursor, b' ');
                self.buf.push(byte);
            } else {
                self.buf[self.cursor] = byte;
            }
            self.cursor += 1;
        }
    }

    /// Consume the line and return its text.
    pub fn finish(self) -> String {
        // The buffer only ever holds the ASCII bytes written through
        // `text`, so this conversion cannot fail.
        String::from_utf8(self.buf).expect("FortranLine holds only ASCII")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- fortran_e ----

    #[test]
    fn e15_8_positive_has_leading_zero_and_blank_sign() {
        // 100.0 → 0.10000000E+03, right-justified in 15 with one
        // blank standing in for the (positive) sign.
        assert_eq!(fortran_e(100.0, 15, 8), " 0.10000000E+03");
    }

    #[test]
    fn e15_8_negative() {
        assert_eq!(fortran_e(-100.0, 15, 8), "-0.10000000E+03");
    }

    #[test]
    fn e15_8_zero() {
        assert_eq!(fortran_e(0.0, 15, 8), " 0.00000000E+00");
    }

    #[test]
    fn e15_8_small_and_large_exponents() {
        // 0.001 → 0.10000000E-02
        assert_eq!(fortran_e(0.001, 15, 8), " 0.10000000E-02");
        // 1.0 → 0.10000000E+01
        assert_eq!(fortran_e(1.0, 15, 8), " 0.10000000E+01");
    }

    #[test]
    fn e15_8_rounds_to_eight_digits() {
        // 1.0/3.0 ≈ 0.33333334 (f32 value, eight mantissa digits).
        let s = fortran_e(1.0 / 3.0, 15, 8);
        assert_eq!(s.len(), 15);
        assert_eq!(s, " 0.33333334E+00");
    }

    #[test]
    fn e_carry_on_round_up() {
        // A value a hair below 100.0: its six-digit mantissa rounds
        // up to 1.000000 and must carry into the exponent — printing
        // .100000E+03, not a seven-digit mantissa or .1000000E+02.
        let just_below_100 = 100.0_f32 - 100.0 * f32::EPSILON;
        assert_eq!(fortran_e(just_below_100, 12, 6), " .100000E+03");
    }

    #[test]
    fn e10_4_drops_leading_zero() {
        // w = 10 = d + 6: no room for the leading 0.
        assert_eq!(fortran_e(100.0, 10, 4), " .1000E+03");
        assert_eq!(fortran_e(-100.0, 10, 4), "-.1000E+03");
    }

    #[test]
    fn e12_6_drops_leading_zero() {
        // w = 12 = d + 6 (the wrtsi SI-report descriptor).
        assert_eq!(fortran_e(0.0, 12, 6), " .000000E+00");
        assert_eq!(fortran_e(1.5, 12, 6), " .150000E+01");
    }

    #[test]
    fn e_too_narrow_overflows_to_asterisks() {
        // w = 8 < d + 6 = 10 cannot hold an E4 value.
        assert_eq!(fortran_e(1.0, 8, 4), "********");
    }

    // ---- fortran_f ----

    #[test]
    fn f3_0_of_zero() {
        // wrthdr's leading dummy record writes rdum = 0.0 with F3.0.
        assert_eq!(fortran_f(0.0, 3, 0), " 0.");
    }

    #[test]
    fn f_with_decimals() {
        assert_eq!(fortran_f(45.678, 8, 2), "   45.68");
        assert_eq!(fortran_f(0.5, 6, 2), "  0.50");
        assert_eq!(fortran_f(-2.0, 7, 1), "   -2.0");
    }

    #[test]
    fn f_too_narrow_overflows() {
        assert_eq!(fortran_f(12345.0, 3, 0), "***");
    }

    // ---- fortran_i ----

    #[test]
    fn i_right_justifies() {
        assert_eq!(fortran_i(25, 5), "   25");
        assert_eq!(fortran_i(-9, 5), "   -9");
        assert_eq!(fortran_i(0, 3), "  0");
        assert_eq!(fortran_i(2020, 4), "2020");
    }

    #[test]
    fn i_too_narrow_overflows() {
        assert_eq!(fortran_i(123456, 4), "****");
    }

    // ---- fortran_a ----

    #[test]
    fn a_pads_short_value_on_the_right() {
        assert_eq!(fortran_a("Cnty", 5), "Cnty ");
        assert_eq!(fortran_a("SCC", 10), "SCC       ");
    }

    #[test]
    fn a_truncates_long_value_to_leftmost() {
        // A4 of the 15-wide name field "UnitsRetro".
        assert_eq!(fortran_a("UnitsRetro", 4), "Unit");
    }

    #[test]
    fn a_exact_width_is_unchanged() {
        assert_eq!(fortran_a("06037", 5), "06037");
    }

    #[test]
    fn a_blank_field() {
        assert_eq!(fortran_a(" ", 5), "     ");
        assert_eq!(fortran_a("", 3), "   ");
    }

    // ---- FortranLine ----

    #[test]
    fn line_tab_pads_a_forward_gap_with_blanks() {
        let mut line = FortranLine::new();
        line.tab(20); // T20
        line.text("Entire U.S.");
        assert_eq!(line.finish(), format!("{}Entire U.S.", " ".repeat(19)));
    }

    #[test]
    fn line_skip_advances_like_nx() {
        let mut line = FortranLine::new();
        line.skip(10); // 10X
        line.text("X");
        assert_eq!(line.finish(), "          X");
    }

    #[test]
    fn line_tab_back_overwrites_existing_content() {
        // wrtsum.f's national line tabs back over already-written
        // text to drop a ':' into it.
        let mut line = FortranLine::new();
        line.text("National Record");
        line.tab(10); // T10 — back into the written text
        line.text(":");
        assert_eq!(line.finish(), "National :ecord");
    }

    #[test]
    fn line_text_at_cursor_then_continues() {
        let mut line = FortranLine::new();
        line.text("ab");
        line.text("cd");
        assert_eq!(line.finish(), "abcd");
    }
}
