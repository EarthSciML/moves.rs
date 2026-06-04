-- County input DB for Washtenaw County (FIPS 26161, zone 261610)
-- Used by process-extended-idle-single, process-apu-single, process-crankcase-extidle-single
--
-- Provides:
-- 1. Required geography tables (State, County, Zone, ZoneRoadType, RoadTypeDistribution)
-- 2. County-specific hotelling data (hotellingHoursPerDay, hotellingActivityDistribution)
--    Values equal national defaults so SINGLE output ≈ DEFAULT at hour 7.

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
