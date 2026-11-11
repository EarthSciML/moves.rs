-- setup-hotelling-multiyear.sql -- Washtenaw County (FIPS 26161, zone 261610),
-- calendar years 2015 AND 2020: ../washtenaw-county-idle-adjust/setup-idle-adjust.sql
-- (the six idle and hotelling ADJUSTMENT tables populated) with every
-- year-keyed table stated at BOTH years.
--
-- Serves fixture: characterization/fixtures/scale-county-hotelling-multiyear.xml
--   (SINGLE domain; 2015 and 2020; April and August, both day types, hours 7
--   and 8; the gasoline passenger car and the diesel combination long-haul
--   truck; road types 1 and 4; Total Energy for running exhaust, extended idle
--   and APU.)
-- Loaded into database `washtenaw_cdb` by run-all-fixtures.sh's
-- county_sql_for -> apptainer/capture-county-snapshot.sh.
--
-- WHY TWO YEARS
-- -------------
-- AdjustHotelling.sql runs once per (zone, year) from TotalActivityGenerator,
-- whose hotellingHours table is cleared only when the PROCESS changes. In the
-- second year it still holds the first year's rows, already adjusted and cut
-- to the RunSpec's hours, and the hour, age and month blocks compute their
-- DEFAULT shares over every row the table holds, and rewrite every row --
-- neither the defaults nor the rewrite filter on yearID. The hotellingHoursPerDay
-- block alone is per year. A one-year county database cannot show this.
--
-- The County Data Manager accepts one year (ImporterManager.java:3151-3182);
-- this fixture skips domain validation, as every county fixture here does,
-- and states each year-keyed table at both years so that MOVES has a
-- population, VMT and fuel supply at each:
--   Year, SourceTypeYear, SourceTypeAgeDistribution, HPMSVtypeYear: the
--     default's rows at 2015 and 2020 (both base years);
--   fuelSupply, fuelUsageFraction: the default's rows at fuel years 2015 and
--     2020 for this county's fuel region;
--   hotellingHoursPerDay: both day types at both years, different totals each;
--   IMCoverage: the default's rows (none).
-- Everything else is setup-idle-adjust.sql verbatim; its header explains each
-- adjustment table's values.
--

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

-- Both years (see the header): the County Data Manager would accept one.
CREATE TABLE IF NOT EXISTS Year LIKE movesdb20241112.Year;
INSERT INTO Year SELECT * FROM movesdb20241112.Year WHERE yearID IN (2015, 2020);

-- ===========================================================================
-- Meteorology (MeteorologyImporter): the zone's 12 months x 24 hours
-- ===========================================================================
CREATE TABLE IF NOT EXISTS ZoneMonthHour LIKE movesdb20241112.ZoneMonthHour;
INSERT INTO ZoneMonthHour SELECT * FROM movesdb20241112.ZoneMonthHour WHERE zoneID = 261610;

-- ===========================================================================
-- Source type population and age distribution (not copied by MOVES at SINGLE)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS SourceTypeYear LIKE movesdb20241112.SourceTypeYear;
INSERT INTO SourceTypeYear SELECT * FROM movesdb20241112.SourceTypeYear WHERE yearID IN (2015, 2020);

CREATE TABLE IF NOT EXISTS SourceTypeAgeDistribution LIKE movesdb20241112.SourceTypeAgeDistribution;
INSERT INTO SourceTypeAgeDistribution SELECT * FROM movesdb20241112.SourceTypeAgeDistribution WHERE yearID IN (2015, 2020);

-- ===========================================================================
-- VMT (VehicleTypeVMTImporter): annual HPMS VMT plus the national fractions
-- ===========================================================================
CREATE TABLE IF NOT EXISTS HPMSVtypeYear LIKE movesdb20241112.HPMSVtypeYear;
INSERT INTO HPMSVtypeYear SELECT * FROM movesdb20241112.HPMSVtypeYear WHERE yearID IN (2015, 2020);

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
 WHERE rc.countyID = 26161 AND rc.fuelYearID IN (2015, 2020) AND rc.regionCodeID = 1
   AND fs.fuelYearID = rc.fuelYearID;

CREATE TABLE IF NOT EXISTS FuelFormulation LIKE movesdb20241112.FuelFormulation;
INSERT INTO FuelFormulation SELECT * FROM movesdb20241112.FuelFormulation;

CREATE TABLE IF NOT EXISTS fuelUsageFraction LIKE movesdb20241112.fuelUsageFraction;
INSERT INTO fuelUsageFraction SELECT * FROM movesdb20241112.fuelUsageFraction
 WHERE countyID = 26161 AND fuelYearID IN (2015, 2020);

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
-- (StartsImporter.sql:334-351). The default database populates
-- StartsPerDayPerVehicle (26 rows) and leaves the other two empty, so that is
-- the one copied, and AdjustStarts takes the same population x
-- startsPerDayPerVehicle branch it takes at DEFAULT scale.
CREATE TABLE IF NOT EXISTS startsPerDayPerVehicle LIKE movesdb20241112.startsPerDayPerVehicle;
INSERT INTO startsPerDayPerVehicle SELECT * FROM movesdb20241112.startsPerDayPerVehicle;

-- Left empty: the two alternatives above (both empty in the default database).
CREATE TABLE IF NOT EXISTS Starts LIKE movesdb20241112.Starts;
CREATE TABLE IF NOT EXISTS startsPerDay LIKE movesdb20241112.startsPerDay;

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
-- Idle (IdleImporter) -- NOT copies; see the header
-- ===========================================================================
CREATE TABLE IF NOT EXISTS totalIdleFraction LIKE movesdb20241112.totalIdleFraction;
INSERT INTO totalIdleFraction SELECT * FROM movesdb20241112.totalIdleFraction;

CREATE TABLE IF NOT EXISTS idleModelYearGrouping LIKE movesdb20241112.idleModelYearGrouping;
INSERT INTO idleModelYearGrouping (sourceTypeID, minModelYearID, maxModelYearID, totalIdleFraction) VALUES
    (21, 1950, 2005, 0.30), (21, 2006, 2014, 0.26), (21, 2015, 2060, 0.22),
    (32, 1950, 2060, 0.20),
    (62, 1950, 2009, 0.18), (62, 2010, 2060, 0.15);

CREATE TABLE IF NOT EXISTS idleMonthAdjust LIKE movesdb20241112.idleMonthAdjust;
INSERT INTO idleMonthAdjust (sourceTypeID, monthID, idleMonthAdjust) VALUES
    (21, 1, 1.00),
    (21, 2, 1.02),
    (21, 3, 1.04),
    (21, 4, 1.08),
    (21, 5, 1.10),
    (21, 6, 1.06),
    (21, 7, 1.03),
    (21, 8, 0.94),
    (21, 9, 0.98),
    (21, 10, 1.01),
    (21, 11, 1.03),
    (21, 12, 1.05),
    (32, 1, 0.95),
    (32, 2, 0.95),
    (32, 3, 0.95),
    (32, 4, 0.95),
    (32, 5, 0.95),
    (32, 6, 0.95),
    (32, 7, 0.95),
    (32, 8, 0.95),
    (32, 9, 0.95),
    (32, 10, 0.95),
    (32, 11, 0.95),
    (32, 12, 0.95);

CREATE TABLE IF NOT EXISTS idleDayAdjust LIKE movesdb20241112.idleDayAdjust;
INSERT INTO idleDayAdjust (sourceTypeID, dayID, idleDayAdjust) VALUES
    (21, 2, 0.92), (21, 5, 1.06),
    (62, 5, 1.12);

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
    (2015, 261610, 2, 1500.0), (2015, 261610, 5, 2900.0),
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
INSERT INTO IMCoverage SELECT * FROM movesdb20241112.IMCoverage WHERE countyID = 26161 AND yearID IN (2015, 2020);

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
