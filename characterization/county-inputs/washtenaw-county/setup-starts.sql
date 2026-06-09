-- County input DB for Washtenaw County (FIPS 26161, zone 261610)
-- Used by process-crankcase-start-single
--
-- Provides:
-- 1. Required geography tables (State, County, Zone, ZoneRoadType, RoadTypeDistribution)
-- 2. County-specific activity: startsPerDay (= national default, so SINGLE ≈ DEFAULT at hour 7)

-- ---- Geography tables (required by MOVES SINGLE-scale validation) ----

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
INSERT INTO zoneRoadType VALUES (261610, 5, 0.002012975189);

CREATE TABLE IF NOT EXISTS roadTypeDistribution (
    sourceTypeID         SMALLINT NOT NULL,
    roadTypeID           SMALLINT NOT NULL,
    roadTypeVMTFraction  DOUBLE,
    PRIMARY KEY (sourceTypeID, roadTypeID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE roadTypeDistribution;
INSERT INTO roadTypeDistribution VALUES (21, 1, 0.000000);
INSERT INTO roadTypeDistribution VALUES (21, 2, 0.081770);
INSERT INTO roadTypeDistribution VALUES (21, 3, 0.204595);
INSERT INTO roadTypeDistribution VALUES (21, 4, 0.259544);
INSERT INTO roadTypeDistribution VALUES (21, 5, 0.454091);

-- Year table: MOVES requires exactly 1 year matching the RunSpec year.
CREATE TABLE IF NOT EXISTS year (
    yearID     SMALLINT NOT NULL,
    isBaseYear CHAR(1),
    fuelYearID SMALLINT,
    PRIMARY KEY (yearID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE year;
INSERT INTO year VALUES (2020, 'Y', 2020);

-- sourceTypeYear: required by MOVES when building RunSpec filter sets at SINGLE scale.
-- Using national default values (MOVES uses startAllocFactor separately for county scaling).
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

-- MOVES checks for unconverted (old-schema) hotelling tables at startup for any
-- ONROAD run, even if hotelling is not used. All 5 tables must exist with the
-- correct schema or MOVES aborts before generating any worker bundles.
-- For starts-only runs, create the required tables with current schema but empty.

CREATE TABLE IF NOT EXISTS hotellingHoursPerDay (
    yearID               SMALLINT NOT NULL,
    zoneID               INT      NOT NULL,
    dayID                SMALLINT NOT NULL,
    hotellingHoursPerDay DOUBLE   NOT NULL,
    PRIMARY KEY (yearID, zoneID, dayID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';

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

CREATE TABLE IF NOT EXISTS hotellingActivityDistribution (
    zoneID           INT      NOT NULL,
    fuelTypeID       SMALLINT NOT NULL,
    beginModelYearID SMALLINT NOT NULL,
    endModelYearID   SMALLINT NOT NULL,
    opModeID         INT      NOT NULL,
    opModeFraction   DOUBLE   NOT NULL,
    PRIMARY KEY (zoneID, fuelTypeID, beginModelYearID, endModelYearID, opModeID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';

CREATE TABLE IF NOT EXISTS hotellingMonthAdjust (
    zoneID          INT      NOT NULL,
    monthID         SMALLINT NOT NULL,
    monthAdjustment DOUBLE   NOT NULL,
    PRIMARY KEY (zoneID, monthID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';

-- ---- Activity tables ----

CREATE TABLE IF NOT EXISTS startsPerDay (
    dayID         SMALLINT NOT NULL DEFAULT 0,
    sourceTypeID  SMALLINT NOT NULL DEFAULT 0,
    startsPerDay  DOUBLE   DEFAULT NULL,
    PRIMARY KEY (sourceTypeID, dayID)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4';
TRUNCATE TABLE startsPerDay;
-- = national sourceTypePopulation × startsPerDayPerVehicle for year 2020
-- Passenger Car (sourceType 21): 105135300 × 3.683876853 = 387305498 (weekday)
--                                 105135300 × 3.130354959 = 329110808 (weekend)
INSERT INTO startsPerDay VALUES (5, 21, 387305498);
INSERT INTO startsPerDay VALUES (2, 21, 329110808);
