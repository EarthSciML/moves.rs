-- setup-retrofit.sql -- thirteen On-Road Retrofit programs (onRoadRetrofit), for
-- the retrofit captures of moves.esm PLAN.md §7.3.79.
--
-- Serves fixtures:
--   characterization/fixtures/scale-county-retrofit.xml
--     (SINGLE domain, Washtenaw 2020; applied AFTER ../washtenaw-county/setup-county.sql,
--     so the scale input database is scale-county's plus this table)
--   characterization/fixtures/rates-retrofit.xml and userdb-retrofit-multiyear.xml
--     (DEFAULT domain; applied ALONE, as the user input database the RunSpec names
--     in <databaseselections>: a county database may hold one calendar year only,
--     so a user database is how a retrofit table reaches a Rates or two-year run)
-- Their twins scale-county-retrofit-base and rates-retrofit-base are the same
-- RunSpecs without this table.
--
-- WHAT MOVES DOES WITH IT (MOVES 5, OnRoadRetrofitStrategy.java)
-- --------------------------------------------------------------
-- MOVESInstantiator.addAutomaticInternalControlStrategies instantiates the
-- strategy whenever the execution database's onRoadRetrofit has a row
-- (InputDataManager copies it filtered by the RunSpec's pollutants, processes,
-- source types and fuel types). compile() expands each row over its model
-- years, keyed (pollutant, process, fuel, source type, retrofitYearID, model
-- year), SUMS rows that share a key, and gives each key the factor
--     1 - sum(cumFractionRetrofit * retrofitEffectiveFraction)
-- (retrofitFactor + nonRetrofitFactor), valid from retrofitYearID until the
-- year before the next retrofitYearID any row names for the same (pollutant,
-- process, fuel, source type, model year). buildSQL multiplies MOVESWorkerOutput
-- (emissionQuant and emissionRate) and BaseRateOutput (emissionRate and
-- meanBaseRate) by it, at those keys, in the worker's SQL AFTER every
-- calculator in the bundle (ExecutionRunSpec.initializeBeforeIteration
-- prepends it to the aggregation SQL; EmissionCalculator writes that after the
-- calculators' processing SQL; the Go external calculator runs on the first
-- statement after its markers).
--
-- THE PROGRAMS (fuel 2, diesel, on every row: the strategy rejects any other
-- fuel, and any source type outside 32, 41, 42, 43, 51, 52, 53, 61, 62, and a
-- single invalid row switches the whole table off)
--   R1-R4  THC running, 62: a phase-in at 2012, 2015, 2018 over overlapping
--          model-year ranges, and a 2021 row that no run year reaches. At 2015,
--          model years 2000-2004 carry R1 and 2005-2014 R2; at 2020, 2000-2004
--          still R1, 2005-2012 R3 and 2013-2014 R2 (a later row REPLACES an
--          earlier one only at the model years it names).
--   R5-R6  CO running, 52: two rows at ONE year, overlapping at 2010-2012, which
--          compile() sums.
--   R7     CO start, 52.
--   R8     NOx running, 42: every vehicle retrofitted, effectiveness NEGATIVE
--          (the importer's own NOTE: "will increase emissions"), factor 1.1.
--   R9-R10 PM2.5 total (110) running and start, 62: a pollutant SulfatePMCalculator
--          SUMS from EC, NonECPM, sulfate and water -- do the species move?
--   R11    EC (112) running, 52: the species alone -- does its total move?
--   R12    THC start, 52, effectiveness 1: do methane, NMHC and VOC, which the
--          HC speciation writes FROM THC, inherit it?
--   R13    NMHC (79) running, 52: a chained child retrofitted on its own.
CREATE TABLE IF NOT EXISTS onRoadRetrofit LIKE movesdb20241112.onRoadRetrofit;
INSERT INTO onRoadRetrofit (pollutantID, processID, fuelTypeID, sourceTypeID, retrofitYearID, beginModelYearID, endModelYearID, cumFractionRetrofit, retrofitEffectiveFraction) VALUES
  (1, 1, 2, 62, 2012, 2000, 2010, 0.20, 0.90),
  (1, 1, 2, 62, 2015, 2005, 2014, 0.40, 0.90),
  (1, 1, 2, 62, 2018, 2005, 2012, 0.70, 0.85),
  (1, 1, 2, 62, 2021, 2000, 2020, 0.90, 0.90),
  (2, 1, 2, 52, 2015, 2000, 2012, 0.60, 0.40),
  (2, 1, 2, 52, 2015, 2010, 2014, 0.20, 0.50),
  (2, 2, 2, 52, 2015, 2000, 2012, 0.60, 0.40),
  (3, 1, 2, 42, 2019, 1995, 2019, 1.00, -0.10),
  (110, 1, 2, 62, 2016, 1990, 2006, 0.75, 0.85),
  (110, 2, 2, 62, 2016, 1990, 2006, 0.75, 0.85),
  (112, 1, 2, 52, 2017, 2000, 2010, 0.40, 0.90),
  (1, 2, 2, 52, 2020, 2010, 2020, 0.25, 1.00),
  (79, 1, 2, 52, 2020, 2005, 2020, 0.50, 0.50);
