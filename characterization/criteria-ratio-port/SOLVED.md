# SOLVED: complete verified criteria-ratio algorithm (NOx, exact for MY1980 & MY1994)

After proving (via compiled canonical evaluator CMHarness) that the literal cmpExpression evaluates to
0.955827, the gap to canon 0.980013 was TWO things I'd assumed wrong:

## THE TWO FIXES
1. **STANDARDIZE by stdDevValue**: each centered fuel-parameter value is divided by its stdDev:
   cmp term = coeff * Π_over_params( (fp.X - center) / stdDev ).  (meanFuelParameters.stdDevValue,
   per polProcess/fuelType/modelYearGroup/fuelParameterID; default 1.0 if NULL or <=0.)
   For products (OxT90 etc.) each factor is standardized: ((Oxy-cO)/stdO)*((T90-cT)/stdT).
   For squares: ((T50-c)/std)^2.
2. **NO sulfur alias** in the predictive ratioNoSulfur: use the TARGET fuel's ACTUAL sulfurLevel
   (7.15), NOT the base's. (The alias is a CO/doCOCalculations thing, not predictive NOx/HC.)

## COMPLETE NOx algorithm (VERIFIED EXACT)
ratioNoSulfur = Σ_fm w_fm·exp( Σ_cmp coeff·standardized_cmp(TARGET) )
              / Σ_fm w_fm·exp( Σ_cmp coeff·standardized_cmp(BASE) )
  - fm in predictNOx models 302-307, w=1/6 each (loadFuelModelIDs: calculationEngines like '%|predictNOx|%').
  - coeff = coeff1+coeff2+coeff3 (complexModelParameters@polProcessID 301).
  - center/stdDev = meanFuelParameters@(301, ft1, modelYearGroup 19502000).
  - base fuel = ff99 (getBaseFormulation predictNOx/ft1/19502000); fp values via fuelParameterName exprs
    (T50=2.0408163*(147.91-E200), T90=4.5454545*(155.47-E300), Oxygen=0.3653*ETOH, Hi=Intercept=1).
  - predictive applies to MY <= ~2000 only (MY2001+ ratioNoSulfur=1.0).
ratio = ratioNoSulfur * sulf_eff
sulf_eff = blend(targetSulfur) / blend(baseSulfur=30); blend(s)=0.5*sa3(s,Normal)+0.5*sa3(s,High)
  - sa3(s,em) = max( sulfShort(s)/sulfShort(sulfurBasis=30), minSulfAdjust=0.50 )
  - sulfShort: Normal(m6=1) log-log exp(coeff*ln(s)); High(m6=2) log-linear exp(coeff*s)
  - sulfurCoeff from sulfurModelCoeff@(pol3,proc1,sourceType21,m6,fuelMYGroup); fuelMYGroup varies by MY
    (19751986 for MY1980-93 -> coeff 0.02083; 19941994 for MY1994 -> different) -> MY1980-93 0.962413 vs MY1994-2000 0.965924.
  - For MY<=2000: sulfurIRFactor=0, sulfurLongCoeff=1 (no M6SulfurCoeff row) so sa3 simplifies as above.
  - For MY>=2001: M6SulfurCoeff has IRFactor/longCoeff (full sulfAdj3 formula); + altCriteriaRatio (HC) + GFR Tier-3.

## VERIFICATION (predictive_model_prototype.py extended)
MY1980: ratioNoSulfur=0.980013 sulf_eff=0.982042 ratio=0.962413 == canon 0.962413  EXACT
MY1994: ratioNoSulfur=0.980013 sulf_eff=0.985624 ratio=0.965924 == canon 0.965924  EXACT

## REMAINING for full port
- CO uses makeAtDifferenceFraction: ratioNoSulfur = 1 + Σw·(exp(t)/exp(b)-1) (same standardized cmps, co models, base=ff99 via 'co' engine). Verify.
- MY>=2001 sulfur model full path (IRFactor/IRR/longCoeff/GPA) + altCriteriaRatio merge (HC) + copy Tier-3 GFR.
- Then Rust: default-DB-only generator writing criteriaRatio scratch (insert-ignore vs snapshot), wire into the criteria/airtoxics/speciation calculators. Verify BOTH gates green.

## Update: ALL 3 CRITERIA POLLUTANTS VERIFIED EXACT (ff9114 MY1980 age40)
- NOx(301): predictive, predictNOx models 302-307, weights @group0 (uniform 1/6), NO sulfur alias -> 0.980013 EXACT
- HC(101):  predictive, predictHC  models, weights @group0 uniform, NO alias -> 1.017098 EXACT
- CO(201):  **atDifferenceFraction** (1+Σw·(exp(t)/exp(b)-1)), 'co' models 1-10, **weights @modelYearGroup 19502000, AGE-SPECIFIC (non-uniform!)**, **WITH sulfur alias** (target sulfur=base) -> 0.983613 EXACT

KEY per-pollutant differences:
- Formula: NOx/HC = makeRatioNoSulfur (Σw·exp/Σw·exp); CO = makeAtDifferenceFraction (1+Σw·(exp(t)/exp(b)-1)).
- Models/weights: NOx/HC weights only @modelYearGroup 0 (uniform); CO weights @19502000/20012050/20512060, per-ageID (non-uniform → criteria ratio is age-dependent for CO).
- Sulfur alias: CO uses it (doCOCalculations aliases ff_target.sulfurLevel->ff_base.sulfurLevel); NOx/HC do NOT.
- Weight lookup: setFuelModelWtFactorVariables queries fuelModelWtFactor @modelYearGroupID then fallback @0, ageID-specific.
- Engine→base: getBaseFormulation(engine, ft, modelYearGroup): predictNOx/predictHC/co all → ff99 for ft1/19502000.

So the ENTIRE predictive+complex fuel-effects model is now reverse-engineered & verified EXACT. Sulfur model also exact (NOx MY1980/1994). REMAINING: MY>=2001 path (predictive→1.0 + full sulfur IRFactor/GPA + altCriteriaRatio HC) + Tier-3 GFR copy + Rust port + wiring.

## Full output structure (for the Rust port)
criteriaRatio is keyed (fuelFormulationID, polProcessID, pollutantID, processID, sourceTypeID, modelYearID, ageID).
- MY<=2000: ratioNoSulfur = predictive/atDiff (per-fuel, source-type-INDEPENDENT); ratio = ratioNoSulfur * sulf_eff. sulf_eff per (sourceType, fuelMYGroup) but for MY<=2000 sulfurModelCoeff is source-type-independent so all 13 sourceTypes share the value (MY1980 = 0.962413 for all). 13 sourceTypes: 11,21,31,32,41,42,43,51,52,53,54,61,62.
- MY>=2001: ratioNoSulfur = 1.0 (predictive doesn't apply). ratio = full sulfur model PER sourceType (M6SulfurCoeff has sulfurIRFactor 0.425, sulfurLongCoeff 1.47, maxIRFactorSulfur per MY-range) -> distinct per sourceType (e.g. MY2001: st11=0.988496, st21/31/32=0.501020, st41+=0.372631). Plus altCriteriaRatio merge (HC 101/102 only, MY>=2001) and copyGeneralFuelRatioToCriteriaRatio Tier-3 (MY>=2017, ratioNoSulfur=1).

## Rust port plan (well-defined, ~600-900 lines, default-DB-only)
1. New fn build_criteria_ratio(store) in default_db_setup.rs, guarded `if criteriaRatio empty` (snapshot path ships it). 
2. Read: complexModelParameters, complexModelParameterName(or hardcode CMP table), meanFuelParameters, fuelModelName, fuelModelWtFactor, baseFuel, fuelParameterName(or hardcode prop exprs), sulfurModelCoeff, M6SulfurCoeff, sulfurBase, FuelFormulation, RunSpecModelYearAge, RunSpecSourceType, RunSpecPollutantProcess(criteria polProcs).
3. For each criteria polProc {101,102,201,202,301,302} x run fuelFormulation x MY-in-range x sourceType x age: compute ratioNoSulfur (this file's ratio_nosulfur) then sulf_eff (this file's sulf_eff for MY<=2000; full IR/GPA for MY>=2001), write criteriaRatio rows.
4. Wire into setup_execution_store (synth_step!). The BaseRateCalculator already applies criteriaRatio (adjust.rs:477) for proc 1/2 — and FuelEffectsGenerator must DROP the criteria polProcs from GFR when criteriaRatio is non-empty (already does, see is_criteria_ratio_pol_process).
5. Verify: compute full criteriaRatio, diff vs snapshot __criteriaratio.parquet; then default_db_snapshot_diff + canonical_snapshot_diff both green.
