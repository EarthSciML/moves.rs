-- setup-avft.sql -- a NON-DEFAULT AVFT (Alternative Vehicle Fuels &
-- Technologies) table for Washtenaw County, applied AFTER a county or project
-- database that states AVFTTool's default projection (run-all-fixtures.sh's
-- county_sql_for lists both; capture-county-snapshot.sh concatenates them).
--
-- Serves fixtures: characterization/fixtures/scale-county-avft.xml (over
-- setup-timeagg.sql) and characterization/fixtures/project-avft.xml (over
-- county-inputs/washtenaw-project-offnetwork/setup-project-offnetwork.sql,
-- where ProjectTAG reads the avft rows directly for hotellingHours).
--
-- WHY THIS RUN
-- ------------
-- MOVES 5 instantiates AVFTControlStrategy whenever the execution database's
-- `avft` table has a row (MOVESInstantiator.addAutomaticInternalControlStrategies),
-- and AVFTMasterLoopable rewrites sampleVehiclePopulation.stmyFraction from it
-- before any generator runs, then reruns UpdateExecution.sql. Every earlier
-- county and project database states AVFTTool's DEFAULT projection
-- (sampleVehiclePopulation summed over regulatory class), which the strategy
-- applies as the identity, so no earlier capture saw the rewrite.
--
-- THE ROWS
-- --------
-- The database under it fills `avft` with the default projection for every source
-- type and model year. This file replaces the rows of the combinations it
-- states and leaves every other row the projection. For each stated
-- (sourceTypeID, modelYearID, fuelTypeID, engTechID) AVFTMasterLoopable.
-- applyNewFraction compares the new fraction (cast to float) with the summed
-- default stmyFraction ("old"):
--   new - old >=  1e-6  ADD:      stmyFraction = stmyFuelEngFraction x new
--   new - old <= -1e-6  SUBTRACT: stmyFraction = stmyFraction x new / old
--   otherwise           nothing; and a combination sampleVehiclePopulation
--                       has no row for is ignored.
-- The rows exercise every branch (defaults from movesdb20241112):
--   21 passenger car       2020  gasoline 0.9408->0.70 (subtract), diesel
--                                 0->0.05 (add from zero), E85 0.017887756->
--                                 0.017888 (+2.4e-7: unchanged), electric
--                                 0.0413->0.232112 (add)
--                          2015  gasoline down, diesel up, E85 +3.9e-7
--                                 (unchanged), electric up
--                          2010  gasoline and E85 restated at their defaults
--                                 (unchanged), diesel ->0 (subtract to zero),
--                                 electric up by diesel's share (add)
--   32 light commercial    2020  gasoline down, diesel up (the add branch
--      truck                      rewrites regulatory classes 30 / 41 from
--                                 stmyFuelEngFraction), E85 -3.6e-9
--                                 (unchanged), electric up
--                          2018  gasoline up by diesel's share, diesel ->0,
--                                 E85 and electric restated (unchanged)
--                          2010  gasoline down, electric 0->0.05 (add from
--                                 zero: regulatory class 30 alone, whose
--                                 stmyFuelEngFraction is 1, class 41's 0);
--                                 diesel and E85 NOT stated (the projection)
--   62 combination long-  2020  diesel 0.9929->0.80, CNG ->0, electric
--      haul truck                 engTech 30 0->0.15 and 40 0->0.05 (add from
--                                 zero: new source bins)
--                          2015  diesel down, CNG up
--                          2018  diesel down, CNG restated (unchanged),
--                                 electric engTech 40 0->9e-7 (inside the
--                                 threshold: stays 0, no bin), and GASOLINE
--                                 0.0017823, a combination sampleVehicle-
--                                 Population has no row for (ignored), so
--                                 this cohort's fractions sum to 0.998217
-- Fractions per (source type, model year) sum to at most 1, as
-- AVFTImporter.sql requires (the run skips domain validation anyway).

DELETE FROM avft
 WHERE (sourceTypeID, modelYearID, fuelTypeID, engTechID) IN (
    (21,2020,1,1),(21,2020,2,1),(21,2020,5,1),(21,2020,9,30),
    (21,2015,1,1),(21,2015,2,1),(21,2015,5,1),(21,2015,9,30),
    (21,2010,1,1),(21,2010,2,1),(21,2010,5,1),(21,2010,9,30),
    (32,2020,1,1),(32,2020,2,1),(32,2020,5,1),(32,2020,9,30),
    (32,2018,1,1),(32,2018,2,1),(32,2018,5,1),(32,2018,9,30),
    (32,2010,1,1),(32,2010,9,30),
    (62,2020,2,1),(62,2020,3,1),(62,2020,9,30),(62,2020,9,40),
    (62,2015,2,1),(62,2015,3,1),
    (62,2018,2,1),(62,2018,3,1),(62,2018,9,40),(62,2018,1,1));

INSERT INTO avft (sourceTypeID, modelYearID, fuelTypeID, engTechID, fuelEngFraction)
VALUES
    (21,2020,1,1,0.700000), (21,2020,2,1,0.050000), (21,2020,5,1,0.0178880), (21,2020,9,30,0.232112),
    (21,2015,1,1,0.850000), (21,2015,2,1,0.100000), (21,2015,5,1,0.0318828), (21,2015,9,30,0.0181172),
    (21,2010,1,1,0.935510183547), (21,2010,2,1,0.0), (21,2010,5,1,0.053674756252), (21,2010,9,30,0.010815059201),
    (32,2020,1,1,0.600000), (32,2020,2,1,0.200000), (32,2020,5,1,0.0968935), (32,2020,9,30,0.1031065),
    (32,2018,1,1,0.85075025334), (32,2018,2,1,0.0), (32,2018,5,1,0.14667605416), (32,2018,9,30,0.0025736925),
    (32,2010,1,1,0.588225662717), (32,2010,9,30,0.050000),
    (62,2020,2,1,0.800000), (62,2020,3,1,0.0), (62,2020,9,30,0.150000), (62,2020,9,40,0.050000),
    (62,2015,2,1,0.940000), (62,2015,3,1,0.060000),
    (62,2018,2,1,0.990000), (62,2018,3,1,0.008216735254), (62,2018,9,40,0.0000009), (62,2018,1,1,0.0017823);
