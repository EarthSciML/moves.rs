//! AVFT Tool — gap-fill + project a partial user AVFT table into a
//! complete one, suitable for feeding the AVFT control strategy
//! (Task 120) or as the canonical `avft` table for a run.
//!
//! Ported from `gov/epa/otaq/moves/master/gui/avfttool/AVFTTool.sql`.
//! The Java implementation is a chain of stored procedures; the Rust
//! port executes them in memory as plain functions over [`AvftTable`].
//!
//! Each invocation of [`run`] does, per source type:
//!
//! 1. **Gap fill** — combine the user's rows for this source type with
//!    the model-default rows (filtered to model years
//!    `1950..=lastCompleteModelYear`). One of four strategies decides
//!    how missing or partial inputs are filled and how the result is
//!    renormalized.
//! 2. **Project** — extend from `lastCompleteModelYear + 1` to
//!    `analysisYear`. One of four strategies decides what fractions
//!    those projection years carry.
//!
//! The final output table is the union of all enabled source types'
//! gap-filled + projected rows. Source types whose
//! [`MethodEntry::enabled`] is `false` are silently dropped from
//! output — matching the Java GUI's checkbox semantics.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{Error, Result};
use crate::model::{
    AvftKey, AvftRecord, AvftTable, EngTechId, FuelTypeId, ModelYearId, SourceTypeId,
};
use crate::spec::{GapFillingMethod, MethodEntry, ProjectionMethod, ToolSpec};

/// Inputs to [`run`].
///
/// `known_fractions` is consulted only when at least one method entry
/// selects [`ProjectionMethod::KnownFractions`]. Pass an empty table
/// (`AvftTable::new()`) if no source type uses that method.
#[derive(Debug, Clone)]
pub struct ToolInputs<'a> {
    pub spec: &'a ToolSpec,
    pub input: &'a AvftTable,
    pub default: &'a AvftTable,
    pub known_fractions: &'a AvftTable,
}

/// A non-fatal message emitted by the tool, mirroring the Java
/// `messages` table.
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// User input did not sum to 1 for a (source type, model year)
    /// group — the gap-filling step is renormalizing.
    Renormalizing {
        source_type_id: SourceTypeId,
        model_year_id: ModelYearId,
        observed_sum: f64,
    },
    /// A source type is absent from the user input file; defaults are
    /// being used for all of it.
    SourceTypeAbsent { source_type_id: SourceTypeId },
    /// `KnownFractions` projection was selected, but the known-fractions
    /// table is missing entries for some projection years for this
    /// source type. The proportional projection covers them instead.
    KnownFractionsIncomplete {
        source_type_id: SourceTypeId,
        missing_years: Vec<ModelYearId>,
    },
}

/// Result of [`run`] — the completed AVFT plus diagnostic messages.
#[derive(Debug, Default)]
pub struct ToolReport {
    pub output: AvftTable,
    pub messages: Vec<Message>,
}

/// Tolerance used by the projection step's "boundary ratio" enforcement
/// — see `AVFTTool_Projection_Proportional` (`@boundaryRatioLimit`).
const BOUNDARY_RATIO_LIMIT: f64 = 2.0;

/// Sum-vs-1 tolerance for the per-group renormalization sanity check.
/// Matches the Java SQL's `ABS(SUM(fuelEngFraction) - 1.0000) > 0.00001`.
const SUM_TOLERANCE: f64 = 0.00001;

/// Run the AVFT Tool: gap-fill + project each enabled source type and
/// concatenate the results.
pub fn run(inputs: &ToolInputs<'_>) -> Result<ToolReport> {
    inputs.spec.validate()?;
    let mut report = ToolReport::default();
    let last_my = inputs.spec.last_complete_model_year;
    let analysis_year = inputs.spec.analysis_year;

    for method in &inputs.spec.methods {
        if !method.enabled {
            continue;
        }
        let source_type_id = method.source_type_id;
        run_source_type(
            method,
            source_type_id,
            last_my,
            analysis_year,
            inputs.input,
            inputs.default,
            inputs.known_fractions,
            &mut report,
        )?;
    }

    Ok(report)
}

#[allow(clippy::too_many_arguments)]
fn run_source_type(
    method: &MethodEntry,
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    analysis_year: ModelYearId,
    input: &AvftTable,
    default: &AvftTable,
    known: &AvftTable,
    report: &mut ToolReport,
) -> Result<()> {
    if !default.iter().any(|r| r.source_type_id == source_type_id) {
        return Err(Error::ToolFailure {
            source_type_id,
            model_year_id: None,
            message: "defaultAVFT has no rows for this sourceTypeID".into(),
        });
    }
    if !input.iter().any(|r| r.source_type_id == source_type_id) {
        report
            .messages
            .push(Message::SourceTypeAbsent { source_type_id });
    }

    let gap_filled = gap_fill(method, source_type_id, last_my, input, default, report)?;

    // Sum check after gap fill — matches the SQL's HAVING ABS(SUM-1)>0.00001 guard.
    enforce_post_gap_fill_sum(source_type_id, &gap_filled, last_my)?;

    project(
        method,
        source_type_id,
        last_my,
        analysis_year,
        &gap_filled,
        default,
        known,
        &mut report.messages,
        &mut report.output,
    )?;
    Ok(())
}

// ============================================================================
// Gap filling
// ============================================================================

fn gap_fill(
    method: &MethodEntry,
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    input: &AvftTable,
    default: &AvftTable,
    report: &mut ToolReport,
) -> Result<AvftTable> {
    match method.gap_filling {
        GapFillingMethod::DefaultsRenormalizeInputs => {
            gap_fill_defaults_renormalize_inputs(source_type_id, last_my, input, default, report)
        }
        GapFillingMethod::DefaultsPreserveInputs => {
            gap_fill_defaults_preserve_inputs(source_type_id, last_my, input, default)
        }
        GapFillingMethod::Automatic => {
            gap_fill_automatic(source_type_id, last_my, input, default, report)
        }
    }
}

/// Fill missing rows from defaults, then rescale user-supplied rows so
/// each (source type, model year) group sums to 1.
fn gap_fill_defaults_renormalize_inputs(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    input: &AvftTable,
    default: &AvftTable,
    report: &mut ToolReport,
) -> Result<AvftTable> {
    let mut filled = build_combined(source_type_id, last_my, input, default);
    renormalize_inputs(&mut filled, report);
    Ok(strip_is_user(filled))
}

/// Fill missing rows from defaults, then rescale the *default* rows so
/// the user-supplied rows are preserved exactly.
fn gap_fill_defaults_preserve_inputs(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    input: &AvftTable,
    default: &AvftTable,
) -> Result<AvftTable> {
    let mut filled = build_combined(source_type_id, last_my, input, default);
    renormalize_defaults(&mut filled);
    Ok(strip_is_user(filled))
}

/// Fill with zeros for any (source type, model year) the user supplied,
/// drop (st, my) groups the user did not touch at all, then fall back
/// to defaults for those untouched groups.
fn gap_fill_automatic(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    input: &AvftTable,
    default: &AvftTable,
    report: &mut ToolReport,
) -> Result<AvftTable> {
    // Step 1: build a renormalized-inputs view of just the user table.
    let mut user_renormed: BTreeMap<AvftKey, f64> = BTreeMap::new();
    let user_sums = group_sums_for_source_type(input, source_type_id);
    for r in input.rows_for_source_type(source_type_id) {
        let s = *user_sums
            .get(&(r.source_type_id, r.model_year_id))
            .unwrap_or(&0.0);
        if s > 0.0 {
            // renormalize but flag warning if sum deviates from 1
            if (s - 1.0).abs() > SUM_TOLERANCE {
                report.messages.push(Message::Renormalizing {
                    source_type_id: r.source_type_id,
                    model_year_id: r.model_year_id,
                    observed_sum: s,
                });
            }
            user_renormed.insert(r.key(), r.fuel_eng_fraction / s);
        }
    }

    // Step 2: zero-fill against defaultAVFT keys (model years 1950..=last_my)
    // — produce the same shape as the SQL's "GapFilling_With0s".
    let mut zero_filled_keys: BTreeSet<AvftKey> = BTreeSet::new();
    for r in default
        .rows_for_source_type(source_type_id)
        .filter(|r| (1950..=last_my).contains(&r.model_year_id))
    {
        zero_filled_keys.insert(r.key());
    }
    // Combine: user-renormed rows take precedence; the rest are 0.
    let mut zero_filled: BTreeMap<AvftKey, f64> = BTreeMap::new();
    for k in &zero_filled_keys {
        let v = user_renormed.get(k).copied().unwrap_or(0.0);
        zero_filled.insert(*k, v);
    }

    // Step 3: drop (sourceType, modelYear) groups whose total fraction is 0
    // — these are "missing model years" we'll let the defaults handle in the
    // subsequent renormalize-inputs pass.
    let mut group_totals: BTreeMap<(SourceTypeId, ModelYearId), f64> = BTreeMap::new();
    for (k, v) in &zero_filled {
        *group_totals
            .entry((k.source_type_id, k.model_year_id))
            .or_insert(0.0) += v;
    }
    zero_filled.retain(|k, _| {
        group_totals
            .get(&(k.source_type_id, k.model_year_id))
            .copied()
            .unwrap_or(0.0)
            > 0.0
    });

    // Re-pack as an AvftTable so the next pass can read it.
    let interim_input: AvftTable = zero_filled
        .into_iter()
        .map(|(k, v)| AvftRecord {
            source_type_id: k.source_type_id,
            model_year_id: k.model_year_id,
            fuel_type_id: k.fuel_type_id,
            eng_tech_id: k.eng_tech_id,
            fuel_eng_fraction: v,
        })
        .collect();

    // Step 4: apply renormalize-inputs gap-fill on the cleaned interim.
    gap_fill_defaults_renormalize_inputs(source_type_id, last_my, &interim_input, default, report)
}

/// Returned by [`build_combined`] for the renormalization step. Tracks
/// each row's provenance so the renormalizers can rescale the user
/// rows vs. the default rows separately.
#[derive(Debug, Clone, Copy)]
struct Combined {
    fraction: f64,
    is_user: bool,
}

/// Build the gap-filled table per the SQL's `LEFT JOIN defaultAVFT`
/// pattern: for every (st, my, fuel, eng) in the default AVFT (model
/// years 1950..=last_my), use the user's value if present, otherwise
/// the default's. Each row keeps track of its provenance.
fn build_combined(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    input: &AvftTable,
    default: &AvftTable,
) -> BTreeMap<AvftKey, Combined> {
    let mut combined: BTreeMap<AvftKey, Combined> = BTreeMap::new();
    for r in default
        .rows_for_source_type(source_type_id)
        .filter(|r| (1950..=last_my).contains(&r.model_year_id))
    {
        let key = r.key();
        let user = input.get(&key);
        let (fraction, is_user) = match user {
            Some(v) => (v, true),
            None => (r.fuel_eng_fraction, false),
        };
        combined.insert(key, Combined { fraction, is_user });
    }
    combined
}

/// Renormalize so the (source type, model year) sum is 1, scaling
/// the user-supplied rows (`is_user = true`) to absorb whatever the
/// defaults don't account for.
///
/// Mirrors `AVFTTool_GapFilling_Defaults_Renormalize_Inputs`.
fn renormalize_inputs(table: &mut BTreeMap<AvftKey, Combined>, report: &mut ToolReport) {
    // Group sums.
    let mut sum_of_defaults: BTreeMap<(SourceTypeId, ModelYearId), f64> = BTreeMap::new();
    let mut sum_of_inputs: BTreeMap<(SourceTypeId, ModelYearId), f64> = BTreeMap::new();
    for (k, v) in table.iter() {
        let group = (k.source_type_id, k.model_year_id);
        if v.is_user {
            *sum_of_inputs.entry(group).or_insert(0.0) += v.fraction;
        } else {
            *sum_of_defaults.entry(group).or_insert(0.0) += v.fraction;
        }
    }
    // Surface renormalization warnings for any group whose user-supplied
    // rows did not sum to (1 - sumOfDefaults).
    let mut groups_seen: BTreeSet<(SourceTypeId, ModelYearId)> = BTreeSet::new();
    for (k, _) in table.iter() {
        let group = (k.source_type_id, k.model_year_id);
        if groups_seen.insert(group) {
            let user_sum = *sum_of_inputs.get(&group).unwrap_or(&0.0);
            if user_sum > 0.0 && (user_sum - 1.0).abs() > SUM_TOLERANCE {
                report.messages.push(Message::Renormalizing {
                    source_type_id: group.0,
                    model_year_id: group.1,
                    observed_sum: user_sum,
                });
            }
        }
    }
    // Rescale.
    for (k, v) in table.iter_mut() {
        if !v.is_user {
            continue;
        }
        let group = (k.source_type_id, k.model_year_id);
        let user_sum = *sum_of_inputs.get(&group).unwrap_or(&0.0);
        let default_sum = *sum_of_defaults.get(&group).unwrap_or(&0.0);
        if user_sum > 0.0 {
            v.fraction = v.fraction / user_sum * (1.0 - default_sum);
        }
    }
}

/// Renormalize so the (source type, model year) sum is 1, scaling
/// the *default-supplied* rows so the user rows are preserved.
///
/// Mirrors `AVFTTool_GapFilling_Defaults_Preserve_Inputs`.
fn renormalize_defaults(table: &mut BTreeMap<AvftKey, Combined>) {
    let mut sum_of_defaults: BTreeMap<(SourceTypeId, ModelYearId), f64> = BTreeMap::new();
    let mut sum_of_inputs: BTreeMap<(SourceTypeId, ModelYearId), f64> = BTreeMap::new();
    for (k, v) in table.iter() {
        let group = (k.source_type_id, k.model_year_id);
        if v.is_user {
            *sum_of_inputs.entry(group).or_insert(0.0) += v.fraction;
        } else {
            *sum_of_defaults.entry(group).or_insert(0.0) += v.fraction;
        }
    }
    for (k, v) in table.iter_mut() {
        if v.is_user {
            continue;
        }
        let group = (k.source_type_id, k.model_year_id);
        let default_sum = *sum_of_defaults.get(&group).unwrap_or(&0.0);
        let input_sum = *sum_of_inputs.get(&group).unwrap_or(&0.0);
        if default_sum > 0.0 {
            v.fraction = v.fraction / default_sum * (1.0 - input_sum);
        }
    }
}

fn strip_is_user(table: BTreeMap<AvftKey, Combined>) -> AvftTable {
    table
        .into_iter()
        .map(|(k, v)| AvftRecord {
            source_type_id: k.source_type_id,
            model_year_id: k.model_year_id,
            fuel_type_id: k.fuel_type_id,
            eng_tech_id: k.eng_tech_id,
            fuel_eng_fraction: v.fraction,
        })
        .collect()
}

/// `(source_type_id, model_year_id)` → SUM of fractions, restricted
/// to one source type. Used by the automatic gap-filling pre-pass.
fn group_sums_for_source_type(
    table: &AvftTable,
    source_type_id: SourceTypeId,
) -> BTreeMap<(SourceTypeId, ModelYearId), f64> {
    let mut sums = BTreeMap::new();
    for r in table.rows_for_source_type(source_type_id) {
        *sums
            .entry((r.source_type_id, r.model_year_id))
            .or_insert(0.0) += r.fuel_eng_fraction;
    }
    sums
}

/// Raise [`Error::ToolFailure`] if any (source type, model year) group
/// in the gap-filled output is nonzero but does not sum to 1.0 (with
/// `SUM_TOLERANCE`). Mirrors the Java tool's post-gap-fill HAVING check.
fn enforce_post_gap_fill_sum(
    source_type_id: SourceTypeId,
    table: &AvftTable,
    _last_my: ModelYearId,
) -> Result<()> {
    let mut sums: BTreeMap<(SourceTypeId, ModelYearId), f64> = BTreeMap::new();
    for r in table.rows_for_source_type(source_type_id) {
        *sums
            .entry((r.source_type_id, r.model_year_id))
            .or_insert(0.0) += r.fuel_eng_fraction;
    }
    for ((st, my), sum) in sums {
        if sum.abs() < SUM_TOLERANCE {
            continue;
        }
        if (sum - 1.0).abs() > SUM_TOLERANCE {
            return Err(Error::ToolFailure {
                source_type_id: st,
                model_year_id: Some(my),
                message: format!(
                    "fuel distribution does not sum to 1 after gap filling (sum={sum})"
                ),
            });
        }
    }
    Ok(())
}

// ============================================================================
// Projection
// ============================================================================

#[allow(clippy::too_many_arguments)]
fn project(
    method: &MethodEntry,
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    analysis_year: ModelYearId,
    gap_filled: &AvftTable,
    default: &AvftTable,
    known: &AvftTable,
    messages: &mut Vec<Message>,
    output: &mut AvftTable,
) -> Result<()> {
    // Always emit the rows up to last_my from the gap-filled input.
    let cap = last_my.min(analysis_year);
    for r in gap_filled
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id <= cap)
    {
        output.insert(r);
    }
    if analysis_year <= last_my {
        // Nothing left to project — last_my is already at or past the
        // analysis year. Match the SQL's behavior of skipping the
        // projection step in this case.
        return Ok(());
    }

    match method.projection {
        ProjectionMethod::Constant => project_constant(
            source_type_id,
            last_my,
            analysis_year,
            gap_filled,
            default,
            output,
        ),
        ProjectionMethod::National => {
            project_national(source_type_id, last_my, analysis_year, default, output)
        }
        ProjectionMethod::Proportional => project_proportional(
            source_type_id,
            last_my,
            analysis_year,
            gap_filled,
            default,
            output,
        ),
        ProjectionMethod::KnownFractions => project_known_fractions(
            source_type_id,
            last_my,
            analysis_year,
            gap_filled,
            default,
            known,
            messages,
            output,
        ),
    }
}

/// Carry the `last_my` row forward by joining with the default's
/// (st, fuel, eng) skeleton for each future model year.
fn project_constant(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    analysis_year: ModelYearId,
    gap_filled: &AvftTable,
    default: &AvftTable,
    output: &mut AvftTable,
) -> Result<()> {
    let baseline: BTreeMap<(FuelTypeId, EngTechId), f64> = gap_filled
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id == last_my)
        .map(|r| ((r.fuel_type_id, r.eng_tech_id), r.fuel_eng_fraction))
        .collect();

    // The SQL `JOIN defaultavft d USING (sourceTypeID, fuelTypeID, engTechID)`
    // — the future-year rows take their (fuel, eng) skeleton from the
    // defaults so we always emit the same (fuel × eng) cardinality as
    // the defaults.
    let future_keys: BTreeSet<(FuelTypeId, EngTechId, ModelYearId)> = default
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id > last_my && r.model_year_id <= analysis_year)
        .map(|r| (r.fuel_type_id, r.eng_tech_id, r.model_year_id))
        .collect();

    for (fuel, eng, my) in future_keys {
        let frac = baseline.get(&(fuel, eng)).copied().unwrap_or(0.0);
        output.insert(AvftRecord::new(source_type_id, my, fuel, eng, frac));
    }
    Ok(())
}

/// Use the defaults verbatim for the projection years.
fn project_national(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    analysis_year: ModelYearId,
    default: &AvftTable,
    output: &mut AvftTable,
) -> Result<()> {
    for r in default
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id > last_my && r.model_year_id <= analysis_year)
    {
        output.insert(r);
    }
    Ok(())
}

/// Scale each default row by the user-vs-default ratio observed at
/// `last_my`, enforce a `[1/boundary, boundary]` clamp, then
/// renormalize each model year to sum to 1.
fn project_proportional(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    analysis_year: ModelYearId,
    gap_filled: &AvftTable,
    default: &AvftTable,
    output: &mut AvftTable,
) -> Result<()> {
    let ratios = baseline_ratios(source_type_id, last_my, gap_filled, default);
    let projected =
        scale_defaults_with_ratios(source_type_id, last_my, analysis_year, default, &ratios);
    let bounded =
        enforce_minimum_boundary(source_type_id, last_my, analysis_year, projected, default);
    let normalized = normalize_per_my(bounded);
    for r in normalized {
        output.insert(r);
    }
    Ok(())
}

/// `AVFTTool_Projection_KnownFractions`: read explicit known projection
/// rows, then fill the rest proportionally and renormalize the
/// non-known share to `1 - knownSum` per model year.
#[allow(clippy::too_many_arguments)]
fn project_known_fractions(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    analysis_year: ModelYearId,
    gap_filled: &AvftTable,
    default: &AvftTable,
    known: &AvftTable,
    messages: &mut Vec<Message>,
    output: &mut AvftTable,
) -> Result<()> {
    // Bucket per (st, my, fuel, eng) with an "is known" flag.
    let mut projected: BTreeMap<AvftKey, (f64, bool)> = BTreeMap::new();

    // Known rows.
    for r in known
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id > last_my && r.model_year_id <= analysis_year)
    {
        projected.insert(r.key(), (r.fuel_eng_fraction, true));
    }

    // Default-derived rows (scaled by baseline ratios) for every default
    // (st, fuel, eng) the known table didn't already cover.
    let ratios = baseline_ratios(source_type_id, last_my, gap_filled, default);
    for r in default
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id > last_my && r.model_year_id <= analysis_year)
    {
        let key = r.key();
        if projected.contains_key(&key) {
            continue;
        }
        let ratio = ratios
            .get(&(r.fuel_type_id, r.eng_tech_id))
            .copied()
            .unwrap_or(1.0)
            .min(BOUNDARY_RATIO_LIMIT);
        let scaled = r.fuel_eng_fraction * ratio;
        projected.insert(key, (scaled, false));
    }

    // Enforce minimum boundary for non-known rows.
    for (k, (frac, is_known)) in projected.iter_mut() {
        if *is_known {
            continue;
        }
        let def = default
            .get(&AvftKey {
                source_type_id: k.source_type_id,
                model_year_id: k.model_year_id,
                fuel_type_id: k.fuel_type_id,
                eng_tech_id: k.eng_tech_id,
            })
            .unwrap_or(*frac);
        let min = def / BOUNDARY_RATIO_LIMIT;
        if *frac < min {
            *frac = min;
        }
    }

    // Group sums.
    let mut known_sum: BTreeMap<ModelYearId, f64> = BTreeMap::new();
    let mut not_known_sum: BTreeMap<ModelYearId, f64> = BTreeMap::new();
    let mut years_with_not_known: BTreeSet<ModelYearId> = BTreeSet::new();
    let mut years_with_known: BTreeSet<ModelYearId> = BTreeSet::new();
    for (k, (v, is_known)) in &projected {
        if *is_known {
            *known_sum.entry(k.model_year_id).or_insert(0.0) += v;
            years_with_known.insert(k.model_year_id);
        } else {
            *not_known_sum.entry(k.model_year_id).or_insert(0.0) += v;
            years_with_not_known.insert(k.model_year_id);
        }
    }
    let missing_known: Vec<ModelYearId> = years_with_not_known
        .difference(&years_with_known)
        .copied()
        .collect();
    if !missing_known.is_empty() {
        messages.push(Message::KnownFractionsIncomplete {
            source_type_id,
            missing_years: missing_known,
        });
    }

    // Renormalize the non-known rows to (1 - knownSum) per model year.
    for (k, (v, is_known)) in projected.iter_mut() {
        if *is_known {
            continue;
        }
        let nk = not_known_sum.get(&k.model_year_id).copied().unwrap_or(0.0);
        let ks = known_sum.get(&k.model_year_id).copied().unwrap_or(0.0);
        if nk > 0.0 {
            *v = *v / nk * (1.0 - ks);
        }
    }

    // Emit.
    for (k, (v, _)) in projected {
        output.insert(AvftRecord {
            source_type_id: k.source_type_id,
            model_year_id: k.model_year_id,
            fuel_type_id: k.fuel_type_id,
            eng_tech_id: k.eng_tech_id,
            fuel_eng_fraction: v,
        });
    }
    Ok(())
}

/// `(fuel, eng) → min(LEAST, COALESCE(input/default, 1))`, computed
/// from the gap-filled `last_my` row vs. the default's `last_my` row.
/// Mirrors the projection SQL's `proportional_scaling_factors` CTE.
fn baseline_ratios(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    gap_filled: &AvftTable,
    default: &AvftTable,
) -> BTreeMap<(FuelTypeId, EngTechId), f64> {
    let user_at_last: BTreeMap<(FuelTypeId, EngTechId), f64> = gap_filled
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id == last_my)
        .map(|r| ((r.fuel_type_id, r.eng_tech_id), r.fuel_eng_fraction))
        .collect();
    let default_at_last: BTreeMap<(FuelTypeId, EngTechId), f64> = default
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id == last_my)
        .map(|r| ((r.fuel_type_id, r.eng_tech_id), r.fuel_eng_fraction))
        .collect();
    let mut ratios = BTreeMap::new();
    for (k, def) in &default_at_last {
        let ratio = match user_at_last.get(k) {
            Some(u) if *def != 0.0 => (u / def).min(BOUNDARY_RATIO_LIMIT),
            _ => 1.0_f64.min(BOUNDARY_RATIO_LIMIT),
        };
        ratios.insert(*k, ratio);
    }
    ratios
}

fn scale_defaults_with_ratios(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    analysis_year: ModelYearId,
    default: &AvftTable,
    ratios: &BTreeMap<(FuelTypeId, EngTechId), f64>,
) -> Vec<AvftRecord> {
    let mut out = Vec::new();
    for r in default
        .rows_for_source_type(source_type_id)
        .filter(|r| r.model_year_id > last_my && r.model_year_id <= analysis_year)
    {
        let ratio = ratios
            .get(&(r.fuel_type_id, r.eng_tech_id))
            .copied()
            .unwrap_or(1.0);
        out.push(AvftRecord {
            source_type_id: r.source_type_id,
            model_year_id: r.model_year_id,
            fuel_type_id: r.fuel_type_id,
            eng_tech_id: r.eng_tech_id,
            fuel_eng_fraction: r.fuel_eng_fraction * ratio,
        });
    }
    out
}

/// Floor each row at `default / boundary` — matches the SQL's
/// `o.fuelEngFraction < d.fuelEngFraction / @boundaryRatioLimit` clamp.
fn enforce_minimum_boundary(
    source_type_id: SourceTypeId,
    last_my: ModelYearId,
    analysis_year: ModelYearId,
    mut rows: Vec<AvftRecord>,
    default: &AvftTable,
) -> Vec<AvftRecord> {
    for r in rows.iter_mut() {
        if r.source_type_id != source_type_id
            || r.model_year_id <= last_my
            || r.model_year_id > analysis_year
        {
            continue;
        }
        let def = default.get(&AvftKey {
            source_type_id: r.source_type_id,
            model_year_id: r.model_year_id,
            fuel_type_id: r.fuel_type_id,
            eng_tech_id: r.eng_tech_id,
        });
        if let Some(d) = def {
            let min = d / BOUNDARY_RATIO_LIMIT;
            if r.fuel_eng_fraction < min {
                r.fuel_eng_fraction = min;
            }
        }
    }
    rows
}

/// Renormalize each (source type, model year) group of projection rows
/// to sum to 1.
fn normalize_per_my(rows: Vec<AvftRecord>) -> Vec<AvftRecord> {
    let mut sums: BTreeMap<(SourceTypeId, ModelYearId), f64> = BTreeMap::new();
    for r in &rows {
        *sums
            .entry((r.source_type_id, r.model_year_id))
            .or_insert(0.0) += r.fuel_eng_fraction;
    }
    rows.into_iter()
        .map(|mut r| {
            let s = *sums
                .get(&(r.source_type_id, r.model_year_id))
                .unwrap_or(&0.0);
            if s > 0.0 {
                r.fuel_eng_fraction /= s;
            }
            r
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{GapFillingMethod, MethodEntry, ProjectionMethod};

    /// Small synthetic default AVFT covering source type 11, model
    /// years 2018..=2022, fuels {1, 2}, single engine tech 1.
    fn default_table() -> AvftTable {
        let mut t = AvftTable::new();
        for my in 2018..=2022 {
            t.insert(AvftRecord::new(11, my, 1, 1, 0.9));
            t.insert(AvftRecord::new(11, my, 2, 1, 0.1));
        }
        t
    }

    #[test]
    fn constant_projection_carries_last_year_forward() {
        let mut input = AvftTable::new();
        // user supplies entire history for fuel 1 = 0.8, fuel 2 = 0.2.
        for my in 2018..=2022 {
            input.insert(AvftRecord::new(11, my, 1, 1, 0.8));
            input.insert(AvftRecord::new(11, my, 2, 1, 0.2));
        }
        let spec = ToolSpec {
            last_complete_model_year: 2022,
            analysis_year: 2024,
            methods: vec![MethodEntry {
                source_type_id: 11,
                enabled: true,
                gap_filling: GapFillingMethod::DefaultsRenormalizeInputs,
                projection: ProjectionMethod::Constant,
            }],
        };
        let default = default_table();
        // For Constant projection, the future-year skeleton comes from
        // `default` (model years > last_my). Extend the default through 2024.
        let mut def = default;
        for my in 2023..=2024 {
            def.insert(AvftRecord::new(11, my, 1, 1, 0.9));
            def.insert(AvftRecord::new(11, my, 2, 1, 0.1));
        }
        let known = AvftTable::new();
        let inputs = ToolInputs {
            spec: &spec,
            input: &input,
            default: &def,
            known_fractions: &known,
        };
        let report = run(&inputs).unwrap();
        // 2023 and 2024 should be 0.8 / 0.2 (the last_my baseline).
        let r2023_1 = report.output.get(&AvftKey {
            source_type_id: 11,
            model_year_id: 2023,
            fuel_type_id: 1,
            eng_tech_id: 1,
        });
        assert!((r2023_1.unwrap() - 0.8).abs() < 1e-9);
        let r2024_2 = report.output.get(&AvftKey {
            source_type_id: 11,
            model_year_id: 2024,
            fuel_type_id: 2,
            eng_tech_id: 1,
        });
        assert!((r2024_2.unwrap() - 0.2).abs() < 1e-9);
    }

    #[test]
    fn national_projection_uses_defaults() {
        let mut input = AvftTable::new();
        for my in 2018..=2022 {
            input.insert(AvftRecord::new(11, my, 1, 1, 0.8));
            input.insert(AvftRecord::new(11, my, 2, 1, 0.2));
        }
        let mut def = default_table();
        for my in 2023..=2024 {
            def.insert(AvftRecord::new(11, my, 1, 1, 0.9));
            def.insert(AvftRecord::new(11, my, 2, 1, 0.1));
        }
        let spec = ToolSpec {
            last_complete_model_year: 2022,
            analysis_year: 2024,
            methods: vec![MethodEntry {
                source_type_id: 11,
                enabled: true,
                gap_filling: GapFillingMethod::DefaultsRenormalizeInputs,
                projection: ProjectionMethod::National,
            }],
        };
        let known = AvftTable::new();
        let inputs = ToolInputs {
            spec: &spec,
            input: &input,
            default: &def,
            known_fractions: &known,
        };
        let report = run(&inputs).unwrap();
        let r2023 = report
            .output
            .get(&AvftKey {
                source_type_id: 11,
                model_year_id: 2023,
                fuel_type_id: 1,
                eng_tech_id: 1,
            })
            .unwrap();
        assert!((r2023 - 0.9).abs() < 1e-9);
    }

    #[test]
    fn gap_fill_renormalize_inputs_rescales_user_share() {
        // User supplies fuel=1 with 0.4; defaults sum to fuel1=0.9, fuel2=0.1.
        // Renormalize-inputs: keep defaults (sum=0.1 from fuel2), rescale
        // user rows so total = 1. User_sum = 0.4, default_sum = 0.1
        // → scaled fuel1 = 0.4 / 0.4 * (1 - 0.1) = 0.9.
        let mut input = AvftTable::new();
        input.insert(AvftRecord::new(11, 2020, 1, 1, 0.4));
        let def = default_table();
        let mut report = ToolReport::default();
        let filled =
            gap_fill_defaults_renormalize_inputs(11, 2022, &input, &def, &mut report).unwrap();
        // Should have entries for 2020 fuel 1 and fuel 2.
        let f1 = filled
            .get(&AvftKey {
                source_type_id: 11,
                model_year_id: 2020,
                fuel_type_id: 1,
                eng_tech_id: 1,
            })
            .unwrap();
        let f2 = filled
            .get(&AvftKey {
                source_type_id: 11,
                model_year_id: 2020,
                fuel_type_id: 2,
                eng_tech_id: 1,
            })
            .unwrap();
        assert!((f1 - 0.9).abs() < 1e-9);
        assert!((f2 - 0.1).abs() < 1e-9);
    }

    #[test]
    fn gap_fill_preserve_inputs_scales_defaults() {
        // User: fuel1=0.6; defaults: fuel1=0.9, fuel2=0.1.
        // Preserve-inputs renormalizes defaults: default_sum=0.1 (fuel2)
        // → fuel2 = 0.1 / 0.1 * (1 - 0.6) = 0.4. Result: fuel1=0.6, fuel2=0.4.
        let mut input = AvftTable::new();
        input.insert(AvftRecord::new(11, 2020, 1, 1, 0.6));
        let def = default_table();
        let filled = gap_fill_defaults_preserve_inputs(11, 2022, &input, &def).unwrap();
        let f1 = filled
            .get(&AvftKey {
                source_type_id: 11,
                model_year_id: 2020,
                fuel_type_id: 1,
                eng_tech_id: 1,
            })
            .unwrap();
        let f2 = filled
            .get(&AvftKey {
                source_type_id: 11,
                model_year_id: 2020,
                fuel_type_id: 2,
                eng_tech_id: 1,
            })
            .unwrap();
        assert!((f1 - 0.6).abs() < 1e-9);
        assert!((f2 - 0.4).abs() < 1e-9);
    }

    #[test]
    fn disabled_source_types_emit_no_output() {
        let mut input = AvftTable::new();
        for my in 2018..=2022 {
            input.insert(AvftRecord::new(11, my, 1, 1, 0.5));
            input.insert(AvftRecord::new(11, my, 2, 1, 0.5));
        }
        let mut def = default_table();
        for my in 2023..=2024 {
            def.insert(AvftRecord::new(11, my, 1, 1, 0.9));
            def.insert(AvftRecord::new(11, my, 2, 1, 0.1));
        }
        let spec = ToolSpec {
            last_complete_model_year: 2022,
            analysis_year: 2024,
            methods: vec![MethodEntry {
                source_type_id: 11,
                enabled: false,
                gap_filling: GapFillingMethod::DefaultsRenormalizeInputs,
                projection: ProjectionMethod::National,
            }],
        };
        let known = AvftTable::new();
        let inputs = ToolInputs {
            spec: &spec,
            input: &input,
            default: &def,
            known_fractions: &known,
        };
        let report = run(&inputs).unwrap();
        assert!(report.output.is_empty());
    }

    #[test]
    fn known_fractions_use_explicit_rows_and_renormalize_remainder() {
        let mut input = AvftTable::new();
        // Match defaults so baseline ratio = 1.
        for my in 2018..=2022 {
            input.insert(AvftRecord::new(11, my, 1, 1, 0.9));
            input.insert(AvftRecord::new(11, my, 2, 1, 0.1));
        }
        let mut def = default_table();
        // Default the projection years too — fuel1=0.9, fuel2=0.1.
        for my in 2023..=2024 {
            def.insert(AvftRecord::new(11, my, 1, 1, 0.9));
            def.insert(AvftRecord::new(11, my, 2, 1, 0.1));
        }
        // Known fractions: in 2024, fuel 2 share rises to 0.5.
        let mut known = AvftTable::new();
        known.insert(AvftRecord::new(11, 2024, 2, 1, 0.5));
        let spec = ToolSpec {
            last_complete_model_year: 2022,
            analysis_year: 2024,
            methods: vec![MethodEntry {
                source_type_id: 11,
                enabled: true,
                gap_filling: GapFillingMethod::DefaultsRenormalizeInputs,
                projection: ProjectionMethod::KnownFractions,
            }],
        };
        let inputs = ToolInputs {
            spec: &spec,
            input: &input,
            default: &def,
            known_fractions: &known,
        };
        let report = run(&inputs).unwrap();
        let r2024_1 = report
            .output
            .get(&AvftKey {
                source_type_id: 11,
                model_year_id: 2024,
                fuel_type_id: 1,
                eng_tech_id: 1,
            })
            .unwrap();
        let r2024_2 = report
            .output
            .get(&AvftKey {
                source_type_id: 11,
                model_year_id: 2024,
                fuel_type_id: 2,
                eng_tech_id: 1,
            })
            .unwrap();
        // fuel 2 is known = 0.5; fuel 1 (not known) renormalizes to 0.5.
        assert!((r2024_2 - 0.5).abs() < 1e-9);
        assert!((r2024_1 - 0.5).abs() < 1e-9);
    }

    #[test]
    fn missing_default_source_type_is_tool_failure() {
        let input = AvftTable::new();
        let def = AvftTable::new();
        let known = AvftTable::new();
        let spec = ToolSpec {
            last_complete_model_year: 2022,
            analysis_year: 2024,
            methods: vec![MethodEntry {
                source_type_id: 11,
                enabled: true,
                gap_filling: GapFillingMethod::DefaultsRenormalizeInputs,
                projection: ProjectionMethod::Constant,
            }],
        };
        let inputs = ToolInputs {
            spec: &spec,
            input: &input,
            default: &def,
            known_fractions: &known,
        };
        match run(&inputs) {
            Err(Error::ToolFailure { source_type_id, .. }) => assert_eq!(source_type_id, 11),
            other => panic!("got {other:?}"),
        }
    }
}
