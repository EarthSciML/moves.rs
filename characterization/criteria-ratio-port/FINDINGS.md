# Criteria-ratio (EPA fuel-effects) port — verification findings

Goal: reproduce canonical `criteriaRatio` for the default-DB path (the port only builds
generalFuelRatio = Tier-3/MY2017+; it's missing the EPA fuel-effects model for all MYs).

## Verified target (canonical snapshot, expand-criteria)
NOx ff 9114, MY1980 (group 19502000), sourceType 21, age 40:
- canonical `ratioNoSulfur` = 0.980013
- canonical `ratio`        = 0.962413
- so sulf_eff = ratio/ratioNoSulfur = 0.98204

## VERIFIED EXACT — the sulfur model
sulf_eff = (target.sulfAdj3 / base.sulfAdj3), 50/50 blend of Normal(m6=1)+High(m6=2) emitters:
- Normal NOx coeff 0.020830, sulfurFunctionID 1 = **log-log**:  sulfShort = exp(coeff·ln(sulfur))
- High   NOx coeff 0.0002848, sulfurFunctionID 2 = **log-linear**: sulfShort = exp(coeff·sulfur)
- sulfAdj3(s) per emitter = max(sulfShort(s)/sulfShort(sulfurBasis=30), minSulfAdjust); minSulfAdjust=0.50 (base sulfur 90>30)
- For MY<=2000: sulfurIRFactor=0, sulfurLongCoeff=1 (no M6SulfurCoeff row) so sulfAdj3 = sulfShort(s)/sulfShort(30).
- target sulfur 7.15 -> blend 0.985... ; base sulfur 30 -> blend 1.0 ; **sulf_eff = 0.98204 EXACT**.
Final ratio = greatest(target.sulfAdj3/base.sulfAdj3, minSulfAdjust) * ratioNoSulfur
(alternate branch when lowSulfurCoeff set: greatest(1-lowSulfurCoeff*(30-sulfur),0)*ratioNoSulfur).

## VERIFIED to 2.5% — the predictive model (ratioNoSulfur)
ratioNoSulfur = Σ_fm[w_fm·exp(target_sum_fm)] / Σ_fm[w_fm·exp(base_sum_fm)] over predictNOx models 302-307 (w=1/6 each).
target_sum/base_sum = Σ_cmp (coeff1+coeff2+coeff3)·cmpExpr(props, center).
- base fuel = ff 99 (baseFuel: predictNOx/ft1/MY1950-2000). NOT meanFuelParameters.baseValue (gives 52x).
- alias: ratioNoSulfur uses BASE fuel's sulfur for the target too (ff_target.sulfurLevel->ff_base.sulfurLevel); sulfur then cancels.
- center = meanFuelParameters.centeringValue at (polProc,fuelType,modelYearGroup); NOT baseValue (gives 0.736).
- **T50 = 2.0408163·(147.91-E200), T90 = 4.5454545·(155.47-E300)** — COMPUTED from E200/E300, NOT raw T50/T90 columns (raw gives 2.32x).
- MY GETS ITS RATIO FROM ITS GROUP'S BASE FUEL + the sulfur model; predictive ratioNoSulfur is MY-group-invariant.
RESULT: my ratioNoSulfur = 0.955827 vs canon 0.980013 (2.5% low). Final ratio 0.938661 vs 0.962413.

## REMAINING 2.5% gap (predictive model) — unresolved
Tested & ruled out: coeff1-only(same), base=baseValue(52x), center=baseValue(0.736), raw T50/T90(2.32x).
All 15 cmpIDs used by NOx (1,2,3,6,7,9,17,21,52,53,54,55,57,58,63) are handled. Per-model exp(t-b): fm302=.9672 fm303=.9105 fm304=.9626 fm305=.9118 fm306=1.0068 fm307=.9734.
The two low models (303,305) pull it down. NEXT: need a canonical per-fuel-model intermediate (target_sum/base_sum) to bisect — not in the snapshot (temp tables dropped). Candidate causes: a centering value for one parameter, a fuel-property unit, or a coeff lookup nuance for models 303/305. Could also instrument the canonical jar if available.

## Then (after closing the 2.5%): CO uses atDifferenceFraction (1+Σw·(exp(t)/exp(b)-1)); output assembly (modelYearGroup/age, sourceType, altCriteriaRatio MY>=2001 HC-only); Rust port as default-DB-only generator writing criteriaRatio scratch with insert-IGNORE (don't clobber snapshot's captured criteriaRatio). Verify BOTH gates green.

## Update: exhaustive verification of the 2.47% predictive gap (all ruled out)
Structural check: **ff99-as-target gives ratioNoSulfur = 1.000000 EXACTLY** → the model/formula is correct; the 2.47% is purely ff9114's property deltas vs ff99.
Verified against canonical source (FuelEffectsGenerator.java) + data, ALL consistent:
- weights = exactly 0.166667 for all 6 models (uniform → cancel); base fuel = ff99 via getBaseFormulation(ft1,19502000).
- center = meanFuelParameters.centeringValue (NOT baseValue; stddev defaults 1.0 and is NOT in the cmp expression).
- coeff = coeff1+coeff2+coeff3 (createComplexModelParameterVariables count=3); no duplicate (polProc,fm,cmp) rows; single dataSourceID.
- T50=2.0408163·(147.91-E200), T90=4.5454545·(155.47-E300) computed (raw T50/T90 cols give 2.32x — wrong).
- ff9114 properties IDENTICAL in default-DB and canonical snapshot (no preprocessing diff).
- ethanol cloning (alterHighEthanolFuelProperties) only touches fuelSubtype 51/52 (E85); ff9114 is subtype 12 (E10) → untouched.
- changeFuelFormulationNulls just nulls→0 (no-op for ff9114).
- all 15 NOx cmpIDs handled; the low models fm303/fm305 are the ones using OxT90(cmp58)/OxT50(cmp63): those interaction diffs (-0.077,-0.073) scaled by (Oxy-centerOxy=2.305) dominate.
CONCLUSION: my ratioNoSulfur 0.955827 vs canon 0.980013 (2.47% low) is unexplained by any static input. NEXT (only path): get canonical RUNTIME per-fuel-model target_sum/base_sum — run the canonical MOVES jar (~/.cache/.../EPA_MOVES_Model) with logging on tempNOxA / the ratioNoSulfur expression, or add a debug dump to FuelEffectsGenerator.doPredictiveCalculations, to bisect which cmp term/value differs. The temp tables are DROPPED so they're not in snapshots.

## Update 2: canonical EVALUATOR harness — my math is PROVEN bit-identical
Compiled the canonical expression package (gov/epa/otaq/moves/common/expression/*) + a harness (CMHarness.java) that replicates makeRatioNoSulfur EXACTLY using the canonical ExpressionParser/Optimizer/Value classes. Fed the verified inputs (coeffs, centers, base ff99, target ff9114, weights, fuelParameter+cmp expressions). Result:
- actual-sulfur:  0.8297439681056681  (== my Python 0.829744)
- alias-sulfur:   0.9558272243845145  (== my Python 0.955827)
So the canonical evaluator REPRODUCES MY EXACT VALUE. My Python math is bit-identical to canonical's expression evaluation. `^` renders to pow() (correct). The 2.47% is NOT a math/evaluator difference.

To compile/run:  cp CMHarness.java to gov/epa/otaq/moves/common/expression/ in ~/.cache/.../EPA_MOVES_Model;
  javac -d /tmp/out -cp ".:libs/commons-lang-2.2.jar" gov/epa/otaq/moves/common/expression/CMHarness.java
  java -cp "/tmp/out:libs/commons-lang-2.2.jar" gov.epa.otaq.moves.common.expression.CMHarness

## Verified EVERYTHING identical, yet production differs — definitive isolation
canon criteriaRatio.ratioNoSulfur for ff9114 NOx: 0.980013 CONSTANT for MY1950-2000; EXACTLY 1.0 for MY2001+ (predictive applies only to MY<=highestFuelPredictiveModelYear ~2000). My exact replication of MY<=2000 = 0.955827.
Verified identical to the captured snapshot: complexModelParameters (0 coeff diffs), meanFuelParameters centers (0 diffs), ff99 + ff9114 properties (0 diffs), models 302-307 (loadFuelModelIDs `like '%|predictNOx|%'`), weights 1/6 (cancel), base=ff99 (getBaseFormulation predictNOx/ft1/19502000), modelYearGroup 19502000 (getModelYearGroupIDs from meanFuelParameters), evaluator (harness). EVERY verifiable input + the evaluation logic are PROVEN identical, yet production=0.980013 vs replication=0.955827.

CONCLUSION: the discrepancy is a production runtime STATE difference NOT reflected in the captured snapshot inputs — something the FuelEffectsGenerator does at runtime between reading these tables and evaluating, that I cannot see statically. The ONLY way to find it: run the FULL canonical MOVES (needs MySQL + RunSpec) with a debug dump of tempNOxA / the optimized expressionText for ff9114 NOx, and diff against CMHarness. CMHarness is the verification oracle for that. Until then the predictive port can't be made bit-exact safely.
