-- setup-timeagg.sql -- Washtenaw County (FIPS 26161, zone 261610), calendar
-- year 2020: the COMPLETE county database of ../washtenaw-county/setup-county.sql
-- with STATED daily starts (startsPerDay) and the four stated hotelling tables
-- of ../washtenaw-county-idle-adjust/setup-idle-adjust.sql, for the two
-- TIME-AGGREGATED county inventories.
--
-- Serves fixtures: characterization/fixtures/scale-county-timeagg-day.xml
--   (SINGLE domain; January and July 2020; both day types; all 24 hours;
--   <aggregateBy key="24-Hour Day"/>, <outputtimestep value="Month"/>)
-- and characterization/fixtures/scale-county-timeagg-month.xml
--   (January, April, July and October; <aggregateBy key="Month"/>,
--   <outputtimestep value="Year"/>)
-- and characterization/fixtures/scale-county-timeagg-year.xml
--   (the same months; <aggregateBy key="Year"/>, <outputtimestep value="Year"/>;
--   also crankcase running and extended idle, brake and tire wear, and
--   running energy and CO2)
-- and characterization/fixtures/scale-county-timeagg-year-toxics.xml and
--   scale-county-timeagg-year-pm.xml (the Year collapse again, source types
--   21 and 62 only: the gaseous species and air toxics at running and start
--   exhaust, and the running-exhaust PM species)
-- and characterization/fixtures/scale-county-timeagg-year-toxics-idle.xml (the
--   same gaseous species at extended idle, APU and the crankcase processes).
-- Both: the gasoline passenger car (21), the gasoline light commercial truck
-- (32) and the diesel combination long-haul truck (62); road types 1 and 4;
-- THC, CO and NOx for running exhaust, start exhaust, crankcase start,
-- extended idle and APU.
-- Loaded into database `washtenaw_cdb` by run-all-fixtures.sh's
-- county_sql_for -> apptainer/capture-county-snapshot.sh.
--
-- WHY THESE RUNS
-- --------------
-- MOVES pre-aggregates the execution database (PreAggDAY.sql, then
-- PreAggMONTH.sql; InputDataManager.preAggregateExecutionDB) BEFORE any
-- generator runs (MasterLoop: "Preaggregate data" precedes
-- MOVESInstantiator.performInstantiation), so TotalActivityGenerator's
-- AdjustStarts.sql and AdjustHotelling.sql run over the COLLAPSED
-- startsPerDay, startsHourFraction, startsOpModeDistribution, sourceTypeHour,
-- hotellingHoursPerDay and hotellingHourFraction. These two runs are where
-- that order shows.
--
-- WHAT IS NOT A COPY, AND WHY
-- ---------------------------
--   startsPerDay  all three source types on both day types, each a different
--     multiple of the default's population x startsPerDayPerVehicle (national
--     scale, as the default is: startAllocFactor allocates it to the zone).
--     AdjustStarts takes the stated branch for EVERY source type of a day type
--     that has a row, so every selected source type has one. PreAggMONTH
--     collapses it to `sum(startsPerDay * dayID / 7)`.
--   hotellingHoursPerDay, hotellingHourFraction, hotellingAgeFraction,
--     hotellingMonthAdjust  setup-idle-adjust.sql's rows verbatim: neither
--     default daily total, a weekend without hour 8, day sums of 0.935 and
--     0.953, ages 0-29, a different adjustment every month. PreAggDAY sums the
--     hour fractions per day; PreAggMONTH renormalises them over the week.
-- Everything else -- including startsHourFraction, startsOpModeDistribution,
-- startsMonthAdjust, startsAgeAdjustment and totalIdleFraction -- is the
-- default database's own rows, copied, as in setup-county.sql.
--
-- Why the copied tables are copies, the AVFT projection, the auditLog flag and
-- the skipped domain validation: see setup-county.sql's header, which holds
-- for this file unchanged.

-- ===========================================================================
-- Geography and time
-- ===========================================================================
CREATE TABLE IF NOT EXISTS State LIKE movesdb20241112.State;
INSERT INTO State SELECT * FROM movesdb20241112.State WHERE stateID = 26;

CREATE TABLE IF NOT EXISTS County LIKE movesdb20241112.County;
INSERT INTO County SELECT * FROM movesdb20241112.County WHERE countyID = 26161;

CREATE TABLE IF NOT EXISTS Zone LIKE movesdb20241112.Zone;
INSERT INTO Zone SELECT * FROM movesdb20241112.Zone WHERE zoneID = 261610;

CREATE TABLE IF NOT EXISTS ZoneRoadType LIKE movesdb20241112.ZoneRoadType;
INSERT INTO ZoneRoadType SELECT * FROM movesdb20241112.ZoneRoadType WHERE zoneID = 261610;

-- MOVES requires exactly one year, the RunSpec's (ImporterManager.java:3151-3182).
CREATE TABLE IF NOT EXISTS Year LIKE movesdb20241112.Year;
INSERT INTO Year SELECT * FROM movesdb20241112.Year WHERE yearID = 2020;

-- ===========================================================================
-- Meteorology (MeteorologyImporter): the zone's 12 months x 24 hours
-- ===========================================================================
CREATE TABLE IF NOT EXISTS ZoneMonthHour LIKE movesdb20241112.ZoneMonthHour;
INSERT INTO ZoneMonthHour SELECT * FROM movesdb20241112.ZoneMonthHour WHERE zoneID = 261610;

-- ===========================================================================
-- Source type population and age distribution (not copied by MOVES at SINGLE)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS SourceTypeYear LIKE movesdb20241112.SourceTypeYear;
INSERT INTO SourceTypeYear SELECT * FROM movesdb20241112.SourceTypeYear WHERE yearID = 2020;

CREATE TABLE IF NOT EXISTS SourceTypeAgeDistribution LIKE movesdb20241112.SourceTypeAgeDistribution;
INSERT INTO SourceTypeAgeDistribution SELECT * FROM movesdb20241112.SourceTypeAgeDistribution WHERE yearID = 2020;

-- ===========================================================================
-- VMT (VehicleTypeVMTImporter): annual HPMS VMT plus the national fractions
-- ===========================================================================
CREATE TABLE IF NOT EXISTS HPMSVtypeYear LIKE movesdb20241112.HPMSVtypeYear;
INSERT INTO HPMSVtypeYear SELECT * FROM movesdb20241112.HPMSVtypeYear WHERE yearID = 2020;

CREATE TABLE IF NOT EXISTS MonthVMTFraction LIKE movesdb20241112.MonthVMTFraction;
INSERT INTO MonthVMTFraction SELECT * FROM movesdb20241112.MonthVMTFraction;

CREATE TABLE IF NOT EXISTS DayVMTFraction LIKE movesdb20241112.DayVMTFraction;
INSERT INTO DayVMTFraction SELECT * FROM movesdb20241112.DayVMTFraction;

CREATE TABLE IF NOT EXISTS HourVMTFraction LIKE movesdb20241112.HourVMTFraction;
INSERT INTO HourVMTFraction SELECT * FROM movesdb20241112.HourVMTFraction;

-- Left empty: the alternative VMT inputs. The default database has no rows in
-- any of them, and a non-empty one would switch MOVES off HPMSVtypeYear
-- (InputDataManager.java:2837-2846).
CREATE TABLE IF NOT EXISTS HPMSVtypeDay LIKE movesdb20241112.HPMSVtypeDay;
CREATE TABLE IF NOT EXISTS SourceTypeYearVMT LIKE movesdb20241112.SourceTypeYearVMT;
CREATE TABLE IF NOT EXISTS SourceTypeDayVMT LIKE movesdb20241112.SourceTypeDayVMT;

-- ===========================================================================
-- Road type distribution and average speed distribution
-- ===========================================================================
CREATE TABLE IF NOT EXISTS RoadTypeDistribution LIKE movesdb20241112.RoadTypeDistribution;
INSERT INTO RoadTypeDistribution SELECT * FROM movesdb20241112.RoadTypeDistribution;

CREATE TABLE IF NOT EXISTS AvgSpeedDistribution LIKE movesdb20241112.AvgSpeedDistribution;
INSERT INTO AvgSpeedDistribution SELECT * FROM movesdb20241112.AvgSpeedDistribution;

-- ===========================================================================
-- Fuel (FuelImporter): supply, formulations, usage, AVFT
-- ===========================================================================
-- fuelSupply is keyed by fuel region; regionCounty maps this county and fuel
-- year to its region (regionCodeID 1 is the fuel region code): 270000000.
CREATE TABLE IF NOT EXISTS fuelSupply LIKE movesdb20241112.fuelSupply;
INSERT INTO fuelSupply
SELECT fs.* FROM movesdb20241112.fuelSupply fs
  JOIN movesdb20241112.regionCounty rc ON rc.regionID = fs.fuelRegionID
 WHERE rc.countyID = 26161 AND rc.fuelYearID = 2020 AND rc.regionCodeID = 1
   AND fs.fuelYearID = 2020;

CREATE TABLE IF NOT EXISTS FuelFormulation LIKE movesdb20241112.FuelFormulation;
INSERT INTO FuelFormulation SELECT * FROM movesdb20241112.FuelFormulation;

CREATE TABLE IF NOT EXISTS fuelUsageFraction LIKE movesdb20241112.fuelUsageFraction;
INSERT INTO fuelUsageFraction SELECT * FROM movesdb20241112.fuelUsageFraction
 WHERE countyID = 26161 AND fuelYearID = 2020;

-- NOT A COPY. MOVES3+ requires a non-empty AVFT (FuelImporter.java:837-858)
-- and the default database ships none: MOVES ships the projection that builds
-- it. This is AVFTTool_CreateDefaultAVFT (AVFTTool.sql:12-14) applied verbatim
-- to the default sampleVehiclePopulation, as in setup-project.sql, whose
-- execution database showed sampleVehiclePopulation still the default's
-- afterwards.
CREATE TABLE IF NOT EXISTS avft LIKE movesdb20241112.avft;
INSERT INTO avft
SELECT sourceTypeID, modelYearID, fuelTypeID, engTechID, SUM(stmyFraction) AS fuelEngFraction
  FROM movesdb20241112.samplevehiclepopulation
 GROUP BY sourceTypeID, modelYearID, fuelTypeID, engTechID;

-- ===========================================================================
-- Starts (StartsImporter)
-- ===========================================================================
-- Exactly one of Starts, StartsPerDay, StartsPerDayPerVehicle may be imported
-- (StartsImporter.sql:334-351); domain validation is skipped. This database
-- states startsPerDay, so AdjustStarts takes its stated branch. The default's
-- StartsPerDayPerVehicle (26 rows) is copied too: MOVES merges it into the
-- execution database in any case, and PreAggMONTH.sql weights
-- startsOpModeDistribution by it whichever branch AdjustStarts takes.
CREATE TABLE IF NOT EXISTS startsPerDayPerVehicle LIKE movesdb20241112.startsPerDayPerVehicle;
INSERT INTO startsPerDayPerVehicle SELECT * FROM movesdb20241112.startsPerDayPerVehicle;

-- Left empty: Starts (empty in the default database).
CREATE TABLE IF NOT EXISTS Starts LIKE movesdb20241112.Starts;
CREATE TABLE IF NOT EXISTS startsPerDay LIKE movesdb20241112.startsPerDay;
-- NOT A COPY: stated daily starts (see the header). The default's
-- population x startsPerDayPerVehicle is 329110808 / 387305498 (21),
-- 45223950 / 52958680 (32) and 531381 / 722084 (62) on days 2 / 5.
INSERT INTO startsPerDay (dayID, sourceTypeID, startsPerDay) VALUES
    (2, 21, 312000000.0), (5, 21, 401000000.0),
    (2, 32,  48000000.0), (5, 32,  51000000.0),
    (2, 62,    610000.0), (5, 62,    700000.0);

CREATE TABLE IF NOT EXISTS startsAgeAdjustment LIKE movesdb20241112.startsAgeAdjustment;
INSERT INTO startsAgeAdjustment SELECT * FROM movesdb20241112.startsAgeAdjustment;

CREATE TABLE IF NOT EXISTS startsHourFraction LIKE movesdb20241112.startsHourFraction;
INSERT INTO startsHourFraction SELECT * FROM movesdb20241112.startsHourFraction;

CREATE TABLE IF NOT EXISTS startsMonthAdjust LIKE movesdb20241112.startsMonthAdjust;
INSERT INTO startsMonthAdjust SELECT * FROM movesdb20241112.startsMonthAdjust;

CREATE TABLE IF NOT EXISTS startsOpModeDistribution LIKE movesdb20241112.startsOpModeDistribution;
INSERT INTO startsOpModeDistribution SELECT * FROM movesdb20241112.startsOpModeDistribution;

-- Left empty: empty in the default database.
CREATE TABLE IF NOT EXISTS importStartsOpModeDistribution LIKE movesdb20241112.importStartsOpModeDistribution;
CREATE TABLE IF NOT EXISTS startsSourceTypeFraction LIKE movesdb20241112.startsSourceTypeFraction;

-- ===========================================================================
-- Idle (IdleImporter)
-- ===========================================================================
-- totalIdleFraction excludes idleModelYearGrouping, idleMonthAdjust and
-- idleDayAdjust (IdleImporter.java:342-367). The default database populates
-- totalIdleFraction (3,120 rows) and leaves the other three empty.
CREATE TABLE IF NOT EXISTS totalIdleFraction LIKE movesdb20241112.totalIdleFraction;
INSERT INTO totalIdleFraction SELECT * FROM movesdb20241112.totalIdleFraction;

-- Left empty: the alternatives above (all empty in the default database).
CREATE TABLE IF NOT EXISTS idleModelYearGrouping LIKE movesdb20241112.idleModelYearGrouping;
CREATE TABLE IF NOT EXISTS idleMonthAdjust LIKE movesdb20241112.idleMonthAdjust;
CREATE TABLE IF NOT EXISTS idleDayAdjust LIKE movesdb20241112.idleDayAdjust;

-- ===========================================================================
-- Hotelling (HotellingImporter) -- NOT copies; see the header
-- ===========================================================================
-- hotellingActivityDistribution: the default's rows at this zone, which are
-- none, so the default's 990000 rows merge untouched (as in setup-county.sql).
CREATE TABLE IF NOT EXISTS hotellingActivityDistribution LIKE movesdb20241112.hotellingActivityDistribution;
INSERT INTO hotellingActivityDistribution
    SELECT * FROM movesdb20241112.hotellingActivityDistribution WHERE zoneID = 261610;

CREATE TABLE IF NOT EXISTS hotellingHoursPerDay LIKE movesdb20241112.hotellingHoursPerDay;
INSERT INTO hotellingHoursPerDay (yearID, zoneID, dayID, hotellingHoursPerDay) VALUES
    (2020, 261610, 2, 1700.0), (2020, 261610, 5, 2600.0);

CREATE TABLE IF NOT EXISTS hotellingHourFraction LIKE movesdb20241112.hotellingHourFraction;
INSERT INTO hotellingHourFraction (zoneID, dayID, hourID, hourFraction) VALUES
    (261610, 2, 1, 0.055),
    (261610, 2, 2, 0.057),
    (261610, 2, 3, 0.058),
    (261610, 2, 4, 0.056),
    (261610, 2, 5, 0.054),
    (261610, 2, 6, 0.051),
    (261610, 2, 7, 0.041),
    (261610, 2, 9, 0.030),
    (261610, 2, 10, 0.028),
    (261610, 2, 11, 0.026),
    (261610, 2, 12, 0.025),
    (261610, 2, 13, 0.025),
    (261610, 2, 14, 0.026),
    (261610, 2, 15, 0.028),
    (261610, 2, 16, 0.030),
    (261610, 2, 17, 0.033),
    (261610, 2, 18, 0.036),
    (261610, 2, 19, 0.040),
    (261610, 2, 20, 0.044),
    (261610, 2, 21, 0.048),
    (261610, 2, 22, 0.052),
    (261610, 2, 23, 0.054),
    (261610, 2, 24, 0.056),
    (261610, 5, 1, 0.062),
    (261610, 5, 2, 0.062),
    (261610, 5, 3, 0.060),
    (261610, 5, 4, 0.058),
    (261610, 5, 5, 0.055),
    (261610, 5, 6, 0.050),
    (261610, 5, 7, 0.034),
    (261610, 5, 8, 0.022),
    (261610, 5, 9, 0.018),
    (261610, 5, 10, 0.016),
    (261610, 5, 11, 0.015),
    (261610, 5, 12, 0.015),
    (261610, 5, 13, 0.015),
    (261610, 5, 14, 0.016),
    (261610, 5, 15, 0.018),
    (261610, 5, 16, 0.021),
    (261610, 5, 17, 0.026),
    (261610, 5, 18, 0.032),
    (261610, 5, 19, 0.040),
    (261610, 5, 20, 0.050),
    (261610, 5, 21, 0.058),
    (261610, 5, 22, 0.062),
    (261610, 5, 23, 0.064),
    (261610, 5, 24, 0.066);

CREATE TABLE IF NOT EXISTS hotellingAgeFraction LIKE movesdb20241112.hotellingAgeFraction;
INSERT INTO hotellingAgeFraction (zoneID, ageID, ageFraction) VALUES
    (261610, 0, 0.055000),
    (261610, 1, 0.051150),
    (261610, 2, 0.047570),
    (261610, 3, 0.044240),
    (261610, 4, 0.041143),
    (261610, 5, 0.038263),
    (261610, 6, 0.035584),
    (261610, 7, 0.033094),
    (261610, 8, 0.030777),
    (261610, 9, 0.028623),
    (261610, 10, 0.026619),
    (261610, 11, 0.024756),
    (261610, 12, 0.023023),
    (261610, 13, 0.021411),
    (261610, 14, 0.019912),
    (261610, 15, 0.018519),
    (261610, 16, 0.017222),
    (261610, 17, 0.016017),
    (261610, 18, 0.014896),
    (261610, 19, 0.013853),
    (261610, 20, 0.012883),
    (261610, 21, 0.011981),
    (261610, 22, 0.011143),
    (261610, 23, 0.010363),
    (261610, 24, 0.009637),
    (261610, 25, 0.008963),
    (261610, 26, 0.008335),
    (261610, 27, 0.007752),
    (261610, 28, 0.007209),
    (261610, 29, 0.006705);

CREATE TABLE IF NOT EXISTS hotellingMonthAdjust LIKE movesdb20241112.hotellingMonthAdjust;
INSERT INTO hotellingMonthAdjust (zoneID, monthID, monthAdjustment) VALUES
    (261610, 1, 0.90),
    (261610, 2, 0.92),
    (261610, 3, 0.95),
    (261610, 4, 1.10),
    (261610, 5, 1.05),
    (261610, 6, 1.00),
    (261610, 7, 0.97),
    (261610, 8, 0.85),
    (261610, 9, 1.02),
    (261610, 10, 1.04),
    (261610, 11, 1.06),
    (261610, 12, 1.08);

-- ===========================================================================
-- I/M (IMImporter) and retrofit (OnRoadRetrofitImporter)
-- ===========================================================================
-- The default's rows for this county and year: none (Michigan has no I/M
-- program in movesdb20241112). Not copied by MOVES at SINGLE scale either.
CREATE TABLE IF NOT EXISTS IMCoverage LIKE movesdb20241112.IMCoverage;
INSERT INTO IMCoverage SELECT * FROM movesdb20241112.IMCoverage WHERE countyID = 26161 AND yearID = 2020;

-- NOT A COPY: the County Data Manager's "No I/M Program" checkbox, as the GUI
-- writes it (TableFileLinkagePart.alterNoDataNeededAuditEntry: importer name
-- "I/M Programs" + " Flag", brief description "No data needed"). Without it
-- the county-domain IM check refuses the empty IMCoverage table.
CREATE TABLE IF NOT EXISTS auditLog (
    whenHappened     DATETIME     NOT NULL,
    importerName     VARCHAR(100) NOT NULL,
    briefDescription VARCHAR(100) DEFAULT NULL,
    fullDescription  VARCHAR(4096) DEFAULT NULL,
    KEY logByDate (whenHappened),
    KEY logByImporter (importerName)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
INSERT INTO auditLog (whenHappened, importerName, briefDescription, fullDescription)
VALUES ('2026-10-20 00:00:00', 'I/M Programs Flag', 'No data needed', '');

-- Left empty: empty in the default database; an empty table is accepted
-- (OnRoadRetrofitImporter.java:224-227).
CREATE TABLE IF NOT EXISTS onRoadRetrofit LIKE movesdb20241112.onRoadRetrofit;
