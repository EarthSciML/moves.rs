-- County input DB for Washtenaw County (FIPS 26161, zone 261610)
-- Used by process-extended-idle-single, process-apu-single, process-crankcase-extidle-single
--
-- Provides:
-- 1. Required geography tables (State, County, Zone, ZoneRoadType, RoadTypeDistribution)
-- 2. County-specific hotelling data (hotellingHoursPerDay, hotellingActivityDistribution)
--    Values equal national defaults so SINGLE output ≈ DEFAULT at hour 7.
--
-- WHY LITERAL INSERTs, AND NOT `CREATE TABLE ... LIKE` + `INSERT ... SELECT`
--
-- The obvious cleanup for this file is to stop hand-transcribing constants and
-- copy them out of the default database instead. That was tried and measured;
-- it does not reproduce this database, so it would invalidate every snapshot
-- captured against it. Recorded here so the experiment is not repeated:
--
--   * Column types diverge in 15 places across 5 tables. The default DB stores
--     roadTypeDistribution.roadTypeVMTFraction, county.GPAFract,
--     county.barometricPressure(CV) and hotellingActivityDistribution.
--     opModeFraction as `float`; these files declare them `double`. `... LIKE`
--     inherits `float` and every value shifts by ~3e-8 relative
--     (e.g. 0.338174 -> 0.33817398548126221). Also char/varchar, smallint/int
--     and NOT NULL/NULL differences, and utf8mb4_uca1400_ai_ci vs
--     utf8mb4_unicode_ci collations.
--   * `... LIKE` also inherits the default DB's secondary indexes: +3 on
--     zone, +3 on roadTypeDistribution, +3 on sourceTypeYear, +1 on
--     zoneRoadType, +1 on year.
--   * The literals here are the default values TRUNCATED TO 12 DECIMAL PLACES
--     (0.001122533244 vs the default 0.00112253324381788, 1.6e-10 relative).
--     The truncation is not cosmetic: it reaches the captured snapshots. The
--     execution-database `zone` table in process-*-single reads
--     1.122533244e-03 where every DEFAULT-scale fixture reads
--     1.12253324381788e-03.
--   * hotellingHoursPerDay, hotellingHours and startsPerDay are all EMPTY in
--     movesdb20241112. There is nothing to SELECT from; those values are
--     derived, not copied.
--   * hotellingActivityDistribution in the default DB exists only for the
--     national placeholder zone 990000, with different model-year bins
--     (2021-2023 / 2024-2026 / 2027-2060 vs the single 2021-9999 bin here).
--
-- So the value here is not "a constant that could have been looked up". Treat
-- these as frozen inputs to already-captured snapshots: changing any of them
-- forces a recapture.
--
-- KNOWN DATA DEFECTS (documented, deliberately NOT fixed here)
--
-- The claim above that hotelling values "equal national defaults" is false for
-- two of the three model-year bins. movesdb20241112 zone 990000 fuelTypeID 2:
--   1950-2009: 200=0.8  201=0     203=0  204=0.2   <- matches
--   2010-2020: 200=0.73 201=0.07  203=0  204=0.2   <- here: 203=0.03 204=0.17
--   2021-2023: 200=0.48 201=0.24  203=0  204=0.28  <- here: 2021-9999 repeats
--                                                     the 2010-2020 row
-- SINGLE output therefore does NOT equal DEFAULT output for 2010+ model years.
--
-- See also EarthSciML/moves.rs#65 (startAllocFactor normalization).

-- ---- Geography tables ----

CREATE TABLE IF NOT EXISTS state (
    stateID        SMALLINT NOT NULL,
    stateName      VARCHAR(50),
    stateAbbr      CHAR(2),
    idleRegionID   SMALLINT,
    PRIMARY KEY (stateID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE state;
INSERT INTO state VALUES (26, 'MICHIGAN', 'MI', 103);

CREATE TABLE IF NOT EXISTS county (
    countyID          INT NOT NULL,
    stateID           SMALLINT NOT NULL,
    countyName        VARCHAR(50),
    altitude          CHAR(1),
    GPAFract          DOUBLE,
    barometricPressure DOUBLE,
    barometricPressureCV DOUBLE,
    countyTypeID      SMALLINT NOT NULL,
    msa               VARCHAR(50),
    PRIMARY KEY (countyID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE county;
INSERT INTO county VALUES (26161, 26, 'Washtenaw County', 'L', 0.0, 29.095, NULL, 1, 'Ann Arbor; MI');

CREATE TABLE IF NOT EXISTS zone (
    zoneID            INT NOT NULL,
    countyID          INT NOT NULL,
    startAllocFactor  DOUBLE,
    idleAllocFactor   DOUBLE,
    SHPAllocFactor    DOUBLE,
    PRIMARY KEY (zoneID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE zone;
INSERT INTO zone VALUES (261610, 26161, 0.001122533244, 0.001122533244, 0.001122533244);

CREATE TABLE IF NOT EXISTS zoneRoadType (
    zoneID           INT NOT NULL,
    roadTypeID       SMALLINT NOT NULL,
    SHOAllocFactor   DOUBLE,
    PRIMARY KEY (zoneID, roadTypeID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE zoneRoadType;
INSERT INTO zoneRoadType VALUES (261610, 2, 0.001310336774);
INSERT INTO zoneRoadType VALUES (261610, 3, 0.000644397243);
INSERT INTO zoneRoadType VALUES (261610, 4, 0.001645627794);
INSERT INTO zoneRoadType VALUES (261610, 5, 0.001045393126);

CREATE TABLE IF NOT EXISTS roadTypeDistribution (
    sourceTypeID         SMALLINT NOT NULL,
    roadTypeID           SMALLINT NOT NULL,
    roadTypeVMTFraction  DOUBLE,
    PRIMARY KEY (sourceTypeID, roadTypeID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE roadTypeDistribution;
-- sourceType 62 (Combination Long-haul Truck)
INSERT INTO roadTypeDistribution VALUES (62, 1, 0.000000);
INSERT INTO roadTypeDistribution VALUES (62, 2, 0.338174);
INSERT INTO roadTypeDistribution VALUES (62, 3, 0.240709);
INSERT INTO roadTypeDistribution VALUES (62, 4, 0.256685);
INSERT INTO roadTypeDistribution VALUES (62, 5, 0.164432);

-- Year table: MOVES requires exactly 1 year matching the RunSpec year.
CREATE TABLE IF NOT EXISTS year (
    yearID     SMALLINT NOT NULL,
    isBaseYear CHAR(1),
    fuelYearID SMALLINT,
    PRIMARY KEY (yearID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE year;
INSERT INTO year VALUES (2020, 'Y', 2020);

-- sourceTypeYear: required by MOVES when building RunSpec filter sets.
CREATE TABLE IF NOT EXISTS sourceTypeYear (
    yearID               SMALLINT NOT NULL,
    sourceTypeID         SMALLINT NOT NULL,
    salesGrowthFactor    DOUBLE,
    sourceTypePopulation DOUBLE,
    migrationRate        DOUBLE,
    PRIMARY KEY (yearID, sourceTypeID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE sourceTypeYear;
INSERT INTO sourceTypeYear VALUES (2020, 11, 0.0, 8347435.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 21, 0.0, 105135300.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 31, 0.0, 135714463.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 32, 0.0, 12829494.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 41, 0.0, 340394.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 42, 0.0, 107199.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 43, 0.0, 483161.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 51, 0.0, 51681.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 52, 0.0, 8445415.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 53, 0.0, 372479.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 54, 0.0, 1038834.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 61, 0.0, 1571399.0, 1.0);
INSERT INTO sourceTypeYear VALUES (2020, 62, 0.0, 1419563.0, 1.0);

-- ---- Hotelling tables ----
-- Values equal national defaults, so AdjustHotelling scales by ratio=1 and
-- hotellingHours remain unchanged. Output at SINGLE scale = DEFAULT scale at hour 7.

CREATE TABLE IF NOT EXISTS hotellingHoursPerDay (
    yearID               SMALLINT NOT NULL,
    zoneID               INT      NOT NULL,
    dayID                SMALLINT NOT NULL,
    hotellingHoursPerDay DOUBLE   NOT NULL,
    PRIMARY KEY (yearID, zoneID, dayID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE hotellingHoursPerDay;
-- defaultDailyHours = sum(hotellingHours) / noOfRealDays from national hotellingHours
INSERT INTO hotellingHoursPerDay VALUES (2020, 261610, 2, 69.606736);
INSERT INTO hotellingHoursPerDay VALUES (2020, 261610, 5, 79.774271);

CREATE TABLE IF NOT EXISTS hotellingHourFraction (
    zoneID       INT      NOT NULL,
    dayID        SMALLINT NOT NULL,
    hourID       SMALLINT NOT NULL,
    hourFraction DOUBLE   NOT NULL,
    PRIMARY KEY (zoneID, dayID, hourID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';

CREATE TABLE IF NOT EXISTS hotellingAgeFraction (
    zoneID      INT      NOT NULL,
    ageID       SMALLINT NOT NULL,
    ageFraction DOUBLE   NOT NULL,
    PRIMARY KEY (zoneID, ageID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';

CREATE TABLE IF NOT EXISTS hotellingMonthAdjust (
    zoneID          INT      NOT NULL,
    monthID         SMALLINT NOT NULL,
    monthAdjustment DOUBLE   NOT NULL,
    PRIMARY KEY (zoneID, monthID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';

CREATE TABLE IF NOT EXISTS hotellingActivityDistribution (
    zoneID           INT      NOT NULL,
    fuelTypeID       SMALLINT NOT NULL,
    beginModelYearID SMALLINT NOT NULL,
    endModelYearID   SMALLINT NOT NULL,
    opModeID         INT      NOT NULL,
    opModeFraction   DOUBLE   NOT NULL,
    PRIMARY KEY (zoneID, fuelTypeID, beginModelYearID, endModelYearID, opModeID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE hotellingActivityDistribution;
-- Diesel (fuelTypeID=2), opMode: 200=ext-idle, 201=APU, 203=shore, 204=all-off
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 1950, 2009, 200, 0.800000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 1950, 2009, 201, 0.000000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 1950, 2009, 203, 0.000000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 1950, 2009, 204, 0.200000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 2010, 2020, 200, 0.730000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 2010, 2020, 201, 0.070000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 2010, 2020, 203, 0.030000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 2010, 2020, 204, 0.170000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 2021, 9999, 200, 0.730000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 2021, 9999, 201, 0.070000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 2021, 9999, 203, 0.030000);
INSERT INTO hotellingActivityDistribution VALUES (261610, 2, 2021, 9999, 204, 0.170000);
