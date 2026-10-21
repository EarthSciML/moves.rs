-- setup-county.sql — a COMPLETE County Data Manager input database for
-- Washtenaw County (FIPS 26161, zone 261610), calendar year 2020.
--
-- Serves fixture: characterization/fixtures/scale-county.xml
--   (SINGLE domain, AUGUST 2020 -- `<month key="7"/>` is a 0-based index, see
--   ../washtenaw-project/setup-project.sql -- weekday, hour 7, passenger car,
--   road types 1 and 4, running exhaust, start exhaust and extended idle
--   energy.)
-- Loaded into database `washtenaw_cdb` by run-all-fixtures.sh's
-- county_sql_for -> apptainer/capture-county-snapshot.sh.
--
-- WHY A NEW FILE, AND NOT setup-starts.sql
-- ----------------------------------------
-- setup-starts.sql and setup-hotelling.sql were written for the four
-- off-network `-single` fixtures, which run with
-- <skipdomaindatabasevalidation selected="true"/>. They hold only what those
-- runs read, and a running-exhaust run cannot validate against them: the
-- County Data Manager's importers need avgSpeedDistribution, fuelFormulation,
-- fuelUsageFraction, AVFT, zoneMonthHour, the VMT fraction tables, the starts
-- tables and totalIdleFraction too. Their literal rows are also FROZEN inputs
-- to already-captured snapshots (their headers say why), so they cannot be
-- widened in place.
--
-- THE CHOICE: EVERY TABLE IS A COPY OF THE DEFAULT DATABASE'S OWN ROWS
-- --------------------------------------------------------------------
-- Every table below is `CREATE TABLE ... LIKE movesdb20241112.<t>` plus
-- `INSERT ... SELECT` from the default database, filtered to THIS county,
-- zone, calendar year and fuel year where the table has such a key, and
-- otherwise copied whole (all months, days, hours and source types, as a real
-- county database states them). This is the setup-project.sql rule, chosen
-- deliberately over setup-starts.sql's literal INSERTs:
--
--   * `LIKE` gives the default database's own column types (FLOAT where the
--     default stores FLOAT) and `SELECT` its own values, so every copied row is
--     the default's row BIT FOR BIT. setup-starts.sql's literals were the
--     default's values truncated to 12 decimals in DOUBLE columns, and that
--     truncation reached its snapshots (zone, zoneRoadType). Nothing here is
--     frozen by an older snapshot, so the faithful copy is free.
--   * The consequence is the point of the fixture: this SINGLE-domain run
--     should reproduce the DEFAULT-domain run of the same selections, cell for
--     cell, wherever MOVES's SINGLE-domain arithmetic is the same arithmetic.
--     Any difference is then MOVES's domain logic (or a merge rule), not the
--     input data.
--   * `LIKE` also brings the default's secondary indexes. They change no
--     value.
--
-- THE TABLES MOVES DOES NOT COPY FROM THE DEFAULT DATABASE AT SINGLE SCALE
-- (InputDataManager.java:1435 and :2834-2846) are the ones that MUST be
-- right here, because there is no default fallback: SourceTypeAgeDistribution,
-- SourceTypeYear, HPMSVtypeYear, FuelSupply and IMCoverage. Every other table
-- is ALSO copied from the default database by MOVES, after this database and
-- by primary key (MasterLoop.java:203-250), so a row here that equals the
-- default's changes nothing.
--
-- Rows that are NOT copies, each justified inline:
--   * AVFT has no default-database rows; it is AVFTTool's own default
--     projection of sampleVehiclePopulation (as in setup-project.sql).
--   * auditLog carries the County Data Manager's "No I/M Program" flag,
--     which is how the GUI records that a county has no I/M program. The
--     default database has no IMCoverage rows for Michigan, so IMCoverage is
--     created EMPTY, and the county-domain IM check (IMCoverageImporter.sql
--     :160-168) refuses an empty table unless that flag is set
--     (IMImporter.java:262, TableFileLinkagePart.java:863-931).
--
-- Tables created EMPTY because the default database has no row for this
-- county/zone (hotelling*), because only one of a set of alternatives may be
-- imported (Starts / StartsPerDay / StartsPerDayPerVehicle,
-- StartsImporter.sql:334-351; totalIdleFraction vs idleModelYearGrouping,
-- idleMonthAdjust, idleDayAdjust, IdleImporter.java:342-367; HPMSVtypeYear vs
-- HPMSVtypeDay / SourceTypeYearVMT / SourceTypeDayVMT), or because the default
-- is empty (onRoadRetrofit), are marked "left empty" with the reason. In each
-- alternative set the copy chosen is the one the default database populates,
-- so MOVES takes the same branch as at DEFAULT scale.
--
-- DOMAIN-DATABASE VALIDATION AND THE GenericImporter ERROR
-- --------------------------------------------------------
-- With this database MOVES's domain validation (MOVESAPI.java:859-878) is
-- the only gate that remains, and it cannot be passed CLEANLY in the pinned
-- SIF: ImporterManager.instantiate() constructs every county importer, and
-- GenericImporter's constructor builds a Swing TableFileLinkagePart whose
-- JComboBox.addItem asks for font metrics; libfontmanager.so needs
-- libharfbuzz.so.0, which the SIF lacks. ImporterInstantiator logs
--   ERROR: ImporterInstantiator is unable to instantiate ...GenericImporter
-- and returns null, and the manager skips the importer (ImporterManager.java
-- :334-337). That is NOT fatal to MOVES -- with this database the run logs
-- "Done performing domain database validation." and simulates to completion
-- -- but it is fatal to apptainer/lib/moves-log-scan.sh, which treats every
-- `ERROR:` line as terminal and keeps an empty allowlist on purpose. The line
-- appears whenever validation runs, whatever tables exist, because it is
-- raised at construction, before any table is read. scale-county.xml
-- therefore sets <skipdomaindatabasevalidation selected="true"/>, like the
-- other five SINGLE/PROJECT fixtures, and the completeness of this database
-- was established by a validation-ON run recorded in the commit that added
-- this file.

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
-- Hotelling (HotellingImporter)
-- ===========================================================================
-- Copies of the default's rows at this zone and year, which are NONE: the
-- default database states hotellingActivityDistribution only for the national
-- placeholder zone 990000 and leaves the other four tables empty. MOVES
-- copies the default's 990000 rows after this database, so the run reads them
-- exactly as at DEFAULT scale. (Only a PROJECT run requires a non-empty
-- hotellingActivityDistribution, HotellingImporter.sql:24-32.)
CREATE TABLE IF NOT EXISTS hotellingActivityDistribution LIKE movesdb20241112.hotellingActivityDistribution;
INSERT INTO hotellingActivityDistribution
    SELECT * FROM movesdb20241112.hotellingActivityDistribution WHERE zoneID = 261610;

CREATE TABLE IF NOT EXISTS hotellingHoursPerDay LIKE movesdb20241112.hotellingHoursPerDay;
INSERT INTO hotellingHoursPerDay
    SELECT * FROM movesdb20241112.hotellingHoursPerDay WHERE zoneID = 261610 AND yearID = 2020;

CREATE TABLE IF NOT EXISTS hotellingHourFraction LIKE movesdb20241112.hotellingHourFraction;
INSERT INTO hotellingHourFraction
    SELECT * FROM movesdb20241112.hotellingHourFraction WHERE zoneID = 261610;

CREATE TABLE IF NOT EXISTS hotellingAgeFraction LIKE movesdb20241112.hotellingAgeFraction;
INSERT INTO hotellingAgeFraction
    SELECT * FROM movesdb20241112.hotellingAgeFraction WHERE zoneID = 261610;

CREATE TABLE IF NOT EXISTS hotellingMonthAdjust LIKE movesdb20241112.hotellingMonthAdjust;
INSERT INTO hotellingMonthAdjust
    SELECT * FROM movesdb20241112.hotellingMonthAdjust WHERE zoneID = 261610;

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
