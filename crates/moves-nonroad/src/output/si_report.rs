//! Spark-ignition (SI) report — `sitot.f` and `wrtsi.f` (Task 114).
//!
//! The optional SI report rolls per-tech-type emissions up into 14
//! output bins for spark-ignition engine reporting. Two Fortran
//! routines drive it:
//!
//! | Fortran | Lines | Rust |
//! |---|---|---|
//! | `sitot.f` | 140 | [`SiReport::accumulate`] |
//! | `wrtsi.f` | 102 | [`write_si_report`] |
//!
//! `sitot.f` maps a record's tech-type name through the
//! [`SI_TECH`]/[`SI_INDEX`]
//! tables (`blknon.f`) to one of [`MXOTCH`] bins and adds the
//! record's population, activity, fuel, and per-pollutant emissions
//! into it. `wrtsi.f` then writes the ten accumulator rows to the SI
//! report file.
//!
//! See the `writers` module for the I/O policy these routines share.

use std::io::{self, Write};

use crate::common::consts::MXPOL;
use crate::emissions::exhaust::PollutantIndex;
use crate::output::fortran_fmt::fortran_e;
use crate::output::statics::{MXOTCH, SI_INDEX, SI_TECH};

/// Running SI-report totals — the Fortran `popsi`, `actsi`,
/// `fuelsi`, and `emissi` accumulator arrays (`nonrdeqp.inc`).
///
/// Each array is indexed by 0-based output bin (`0..`[`MXOTCH`]).
/// [`SiReport::default`] is the all-zero starting state `blknon.f`
/// initialises.
#[derive(Debug, Clone)]
pub struct SiReport {
    /// Population total per output bin (`popsi`).
    pub population: [f32; MXOTCH],
    /// Activity total per output bin (`actsi`).
    pub activity: [f32; MXOTCH],
    /// Fuel-consumption total per output bin (`fuelsi`).
    pub fuel: [f32; MXOTCH],
    /// Per-pollutant emission totals per output bin (`emissi`):
    /// `emissions[bin][pollutant_slot]`.
    pub emissions: [[f32; MXPOL]; MXOTCH],
}

impl Default for SiReport {
    fn default() -> Self {
        SiReport {
            population: [0.0; MXOTCH],
            activity: [0.0; MXOTCH],
            fuel: [0.0; MXOTCH],
            emissions: [[0.0; MXPOL]; MXOTCH],
        }
    }
}

/// Outcome of feeding one record to [`SiReport::accumulate`] —
/// `sitot.f`'s `ISUCES`/`ISKIP` return codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiOutcome {
    /// The record's tech type is one of the 42 SI-tracked types and
    /// its totals were added to a bin (`ISUCES`).
    Accumulated,
    /// The record's tech type is not SI-tracked; the record
    /// contributes nothing (`ISKIP`).
    Skipped,
}

impl SiReport {
    /// Add one record's totals to the matching SI bin — `sitot.f`.
    ///
    /// The record's `tech_type` is matched against the [`SI_TECH`]
    /// table; an unmatched type returns [`SiOutcome::Skipped`] and
    /// leaves the accumulator untouched. A negative population,
    /// activity, fuel, or emission value is NONROAD's "no data"
    /// marker and is not added.
    pub fn accumulate(
        &mut self,
        tech_type: &str,
        population: f32,
        activity: f32,
        fuel: f32,
        emissions: &[f32; MXPOL],
    ) -> SiOutcome {
        // sitot.f :76–80 — fndchr lookup of the tech type. The
        // Fortran compares the 10-character (blank-padded) fields, so
        // trailing blanks on the record's name are not significant.
        let key = tech_type.trim_end();
        let Some(table_idx) = SI_TECH.iter().position(|&t| t == key) else {
            return SiOutcome::Skipped;
        };
        // sitot.f :84 — the indxsi entry maps the input slot to a
        // 1-based output bin; with the static SI_INDEX table it is
        // always within range, so the Fortran's `7000` out-of-range
        // error path is unreachable.
        let bin = usize::from(SI_INDEX[table_idx]) - 1;
        debug_assert!(bin < MXOTCH, "SI_INDEX entry exceeds MXOTCH");

        // sitot.f :88–110 — accumulate, skipping the no-data marker.
        if population >= 0.0 {
            self.population[bin] += population;
        }
        if activity >= 0.0 {
            self.activity[bin] += activity;
        }
        if fuel >= 0.0 {
            self.fuel[bin] += fuel;
        }
        for (slot, &value) in emissions.iter().enumerate() {
            if value >= 0.0 {
                self.emissions[bin][slot] += value;
            }
        }
        SiOutcome::Accumulated
    }
}

/// Write one SI-report row: [`MXOTCH`] values, `E12.6`,
/// comma-separated — `wrtsi.f` format `9000`.
fn write_si_row<W: Write>(w: &mut W, values: &[f32; MXOTCH]) -> io::Result<()> {
    let mut line = String::new();
    for (i, &value) in values.iter().enumerate() {
        if i > 0 {
            line.push(',');
        }
        line.push_str(&fortran_e(value, 12, 6));
    }
    writeln!(w, "{line}")
}

/// Extract one pollutant's column across all [`MXOTCH`] bins.
fn pollutant_column(report: &SiReport, pollutant: PollutantIndex) -> [f32; MXOTCH] {
    let slot = pollutant.slot();
    let mut column = [0.0; MXOTCH];
    for (bin, value) in column.iter_mut().enumerate() {
        *value = report.emissions[bin][slot];
    }
    column
}

/// Write the SI report — `wrtsi.f`.
///
/// Emits the ten accumulator rows in `wrtsi.f`'s fixed order:
/// population, activity, fuel, then exhaust THC, crankcase, diurnal,
/// and displacement emissions, then a row of zeros (the Fortran
/// writes a literal `0.0` row — a placeholder column), then NOx and
/// CO exhaust emissions.
pub fn write_si_report<W: Write>(w: &mut W, report: &SiReport) -> io::Result<()> {
    write_si_row(w, &report.population)?;
    write_si_row(w, &report.activity)?;
    write_si_row(w, &report.fuel)?;
    write_si_row(w, &pollutant_column(report, PollutantIndex::Thc))?;
    write_si_row(w, &pollutant_column(report, PollutantIndex::Crankcase))?;
    write_si_row(w, &pollutant_column(report, PollutantIndex::Diurnal))?;
    write_si_row(w, &pollutant_column(report, PollutantIndex::Displacement))?;
    write_si_row(w, &[0.0; MXOTCH])?;
    write_si_row(w, &pollutant_column(report, PollutantIndex::Nox))?;
    write_si_row(w, &pollutant_column(report, PollutantIndex::Co))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zero_emissions() -> [f32; MXPOL] {
        [0.0; MXPOL]
    }

    // ---- sitot ----

    #[test]
    fn accumulate_maps_tech_type_to_its_bin() {
        let mut report = SiReport::default();
        // "G2N1" is SI_TECH[0] → SI_INDEX[0] = 1 → bin 0.
        let outcome = report.accumulate("G2N1", 100.0, 50.0, 10.0, &zero_emissions());
        assert_eq!(outcome, SiOutcome::Accumulated);
        assert_eq!(report.population[0], 100.0);
        assert_eq!(report.activity[0], 50.0);
        assert_eq!(report.fuel[0], 10.0);
    }

    #[test]
    fn accumulate_groups_three_tech_types_into_one_bin() {
        let mut report = SiReport::default();
        // "G2N1", "G2N11", "G2N12" all map to bin 0.
        report.accumulate("G2N1", 1.0, 0.0, 0.0, &zero_emissions());
        report.accumulate("G2N11", 2.0, 0.0, 0.0, &zero_emissions());
        report.accumulate("G2N12", 4.0, 0.0, 0.0, &zero_emissions());
        assert_eq!(report.population[0], 7.0);
    }

    #[test]
    fn accumulate_skips_untracked_tech_type() {
        let mut report = SiReport::default();
        let outcome = report.accumulate("DIESEL", 100.0, 0.0, 0.0, &zero_emissions());
        assert_eq!(outcome, SiOutcome::Skipped);
        assert_eq!(report.population, [0.0; MXOTCH]);
    }

    #[test]
    fn accumulate_ignores_trailing_blanks_in_tech_type() {
        let mut report = SiReport::default();
        // The Fortran compares 10-character padded fields.
        assert_eq!(
            report.accumulate("G4H5C2    ", 1.0, 0.0, 0.0, &zero_emissions()),
            SiOutcome::Skipped // not a real code — but exercises trimming
        );
        assert_eq!(
            report.accumulate("G2H5C2", 1.0, 0.0, 0.0, &zero_emissions()),
            SiOutcome::Accumulated
        );
        assert_eq!(
            report.accumulate("G2H5C2  ", 1.0, 0.0, 0.0, &zero_emissions()),
            SiOutcome::Accumulated
        );
    }

    #[test]
    fn accumulate_skips_negative_no_data_values() {
        let mut report = SiReport::default();
        let mut emissions = zero_emissions();
        emissions[0] = -9.0; // no-data marker
        emissions[1] = 5.0;
        report.accumulate("G2N1", -9.0, -9.0, -9.0, &emissions);
        // The negative population/activity/fuel are not accumulated.
        assert_eq!(report.population[0], 0.0);
        assert_eq!(report.activity[0], 0.0);
        assert_eq!(report.fuel[0], 0.0);
        // The negative emission slot is skipped, the positive added.
        assert_eq!(report.emissions[0][0], 0.0);
        assert_eq!(report.emissions[0][1], 5.0);
    }

    #[test]
    fn accumulate_last_tech_type_maps_to_last_bin() {
        let mut report = SiReport::default();
        // "G2H5C2" is SI_TECH[41] → SI_INDEX[41] = 14 → bin 13.
        report.accumulate("G2H5C2", 1.0, 0.0, 0.0, &zero_emissions());
        assert_eq!(report.population[MXOTCH - 1], 1.0);
    }

    // ---- wrtsi ----

    #[test]
    fn si_report_has_ten_rows() {
        let report = SiReport::default();
        let mut buf = Vec::new();
        write_si_report(&mut buf, &report).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out.lines().count(), 10);
        // Every row carries MXOTCH values ⇒ MXOTCH-1 commas.
        for row in out.lines() {
            assert_eq!(row.matches(',').count(), MXOTCH - 1);
        }
    }

    #[test]
    fn si_report_rows_carry_the_accumulated_values() {
        let mut report = SiReport::default();
        let mut emissions = zero_emissions();
        emissions[PollutantIndex::Nox.slot()] = 42.0;
        report.accumulate("G2N1", 100.0, 0.0, 0.0, &emissions);

        let mut buf = Vec::new();
        write_si_report(&mut buf, &report).unwrap();
        let out = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        // Row 1 is population; bin 0 holds 100.0.
        assert!(lines[0].starts_with(&fortran_e(100.0, 12, 6)));
        // Row 9 is NOx exhaust; bin 0 holds 42.0.
        assert!(lines[8].starts_with(&fortran_e(42.0, 12, 6)));
        // Row 8 is the literal zero placeholder row.
        assert!(lines[7].starts_with(&fortran_e(0.0, 12, 6)));
    }
}
