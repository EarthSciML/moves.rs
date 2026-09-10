-- setup-project.sql — PROJECT-domain scale input database for Washtenaw County
-- (FIPS 26161, zone 261610, link 1, road type 4, July 2020, hour 8, day 5).
--
-- Serves fixture: characterization/fixtures/scale-project.xml
-- Loaded into database `washtenaw_cdb` by
--   apptainer/capture-county-snapshot.sh --fixture scale-project \
--       --county-sql characterization/county-inputs/washtenaw-project/setup-project.sql
--
-- WHY THIS FIXTURE EXISTS
-- -----------------------
-- `LinkOperatingModeDistributionGenerator` is reachable only in PROJECT
-- domain: it is on the DO_RATES_FIRST whitelist in
-- MOVESInstantiator.java:1454-1476, and it is the only code path that
-- writes a non-empty `opModeDistribution` with isUserInput='N'. Every other
-- fixture in the corpus captures that table empty, so no snapshot could
-- constrain the generator's port. This input DB is the minimum that makes
-- canonical MOVES run the generator.
--
-- AUTHORING RULE — NO HAND-TRANSCRIBED CONSTANTS
-- ----------------------------------------------
-- Every table below is built with `CREATE TABLE ... LIKE` against the
-- default database, and populated with `INSERT ... SELECT` filtered to this
-- fixture's geography and time. Nothing is retyped from the default DB, so
-- the file cannot drift from it and carries no transcription errors.
-- (Contrast washtenaw-county/setup-{starts,hotelling}.sql, which predate
-- this rule and spell out both DDL and rows by hand.)
--
-- The ONLY invented rows are the three the default database cannot supply,
-- each justified inline below: `link`, `linkSourceTypeHour`, and
-- `offNetworkLink`. A project *is* its links, and the default database has
-- no links; there is nothing to select from.
--
-- Tables deliberately left EMPTY after creation are marked "left empty"
-- with the reason. An empty-but-present table is not the same as an absent
-- one: MOVES's project importer requires the table to exist, and an empty
-- one is the signal that MOVES should compute the contents itself.

-- ===========================================================================
-- Geography and time
-- ===========================================================================
CREATE TABLE IF NOT EXISTS State LIKE movesdb20241112.State;
INSERT INTO State SELECT * FROM movesdb20241112.State WHERE stateID=26;

CREATE TABLE IF NOT EXISTS County LIKE movesdb20241112.County;
INSERT INTO County SELECT * FROM movesdb20241112.County WHERE countyID=26161;

CREATE TABLE IF NOT EXISTS Zone LIKE movesdb20241112.Zone;
INSERT INTO Zone SELECT * FROM movesdb20241112.Zone WHERE zoneID=261610;

CREATE TABLE IF NOT EXISTS ZoneRoadType LIKE movesdb20241112.ZoneRoadType;
INSERT INTO ZoneRoadType SELECT * FROM movesdb20241112.ZoneRoadType WHERE zoneID=261610;

CREATE TABLE IF NOT EXISTS Year LIKE movesdb20241112.Year;
INSERT INTO Year SELECT * FROM movesdb20241112.Year WHERE yearID=2020;

-- Meteorology for the zone: all months/hours, so MeteorologyGenerator sees
-- the same source rows it would in a DEFAULT-domain run.
CREATE TABLE IF NOT EXISTS ZoneMonthHour LIKE movesdb20241112.ZoneMonthHour;
INSERT INTO ZoneMonthHour SELECT * FROM movesdb20241112.ZoneMonthHour WHERE zoneID=261610;

-- ===========================================================================
-- Fleet: age distribution, fuel, AVFT
-- ===========================================================================
CREATE TABLE IF NOT EXISTS SourceTypeAgeDistribution LIKE movesdb20241112.SourceTypeAgeDistribution;
INSERT INTO SourceTypeAgeDistribution SELECT * FROM movesdb20241112.SourceTypeAgeDistribution WHERE yearID=2020;

CREATE TABLE IF NOT EXISTS FuelFormulation LIKE movesdb20241112.FuelFormulation;
INSERT INTO FuelFormulation SELECT * FROM movesdb20241112.FuelFormulation;

CREATE TABLE IF NOT EXISTS fuelUsageFraction LIKE movesdb20241112.fuelUsageFraction;
INSERT INTO fuelUsageFraction SELECT * FROM movesdb20241112.fuelUsageFraction
  WHERE countyID=26161 AND fuelYearID=2020;

-- fuelSupply is keyed by fuelRegionID, not countyID; regionCounty maps this
-- county+fuelYear to its region (regionCodeID=1 is the fuel region code).
CREATE TABLE IF NOT EXISTS fuelSupply LIKE movesdb20241112.fuelSupply;
INSERT INTO fuelSupply
SELECT fs.* FROM movesdb20241112.fuelSupply fs
  JOIN movesdb20241112.regionCounty rc ON rc.regionID = fs.fuelRegionID
 WHERE rc.countyID = 26161 AND rc.fuelYearID = 2020 AND rc.regionCodeID = 1;

-- AVFT has no default-database table to copy: MOVES ships the projection
-- that builds it. This is AVFTTool_CreateDefaultAVFT from AVFTTool.sql
-- (lines 12-14) applied verbatim to the default samplevehiclepopulation.
-- Derived by the model's own query, not transcribed.
CREATE TABLE IF NOT EXISTS avft LIKE movesdb20241112.avft;
INSERT INTO avft
SELECT sourceTypeID, modelYearID, fuelTypeID, engTechID, SUM(stmyFraction) AS fuelEngFraction
  FROM movesdb20241112.samplevehiclepopulation
 GROUP BY sourceTypeID, modelYearID, fuelTypeID, engTechID;

-- ===========================================================================
-- Hotelling and idling (required by the project importer even though this
-- fixture selects no hotelling process)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS hotellingActivityDistribution LIKE movesdb20241112.hotellingActivityDistribution;
INSERT INTO hotellingActivityDistribution SELECT * FROM movesdb20241112.hotellingActivityDistribution WHERE zoneID=261610;

CREATE TABLE IF NOT EXISTS hotellingAgeFraction LIKE movesdb20241112.hotellingAgeFraction;
INSERT INTO hotellingAgeFraction SELECT * FROM movesdb20241112.hotellingAgeFraction WHERE zoneID=261610;

CREATE TABLE IF NOT EXISTS hotellingHourFraction LIKE movesdb20241112.hotellingHourFraction;
INSERT INTO hotellingHourFraction SELECT * FROM movesdb20241112.hotellingHourFraction WHERE zoneID=261610;

CREATE TABLE IF NOT EXISTS hotellingHoursPerDay LIKE movesdb20241112.hotellingHoursPerDay;
INSERT INTO hotellingHoursPerDay SELECT * FROM movesdb20241112.hotellingHoursPerDay WHERE zoneID=261610 AND yearID=2020;

CREATE TABLE IF NOT EXISTS hotellingMonthAdjust LIKE movesdb20241112.hotellingMonthAdjust;
INSERT INTO hotellingMonthAdjust SELECT * FROM movesdb20241112.hotellingMonthAdjust WHERE zoneID=261610 AND monthID=7;

CREATE TABLE IF NOT EXISTS hotellingHours LIKE movesdb20241112.hotellingHours;
INSERT INTO hotellingHours SELECT * FROM movesdb20241112.hotellingHours WHERE zoneID=261610 AND yearID=2020 AND monthID=7;

CREATE TABLE IF NOT EXISTS idleDayAdjust LIKE movesdb20241112.idleDayAdjust;
INSERT INTO idleDayAdjust SELECT * FROM movesdb20241112.idleDayAdjust;

CREATE TABLE IF NOT EXISTS idleModelYearGrouping LIKE movesdb20241112.idleModelYearGrouping;
INSERT INTO idleModelYearGrouping SELECT * FROM movesdb20241112.idleModelYearGrouping;

CREATE TABLE IF NOT EXISTS idleMonthAdjust LIKE movesdb20241112.idleMonthAdjust;
INSERT INTO idleMonthAdjust SELECT * FROM movesdb20241112.idleMonthAdjust WHERE monthID=7;

CREATE TABLE IF NOT EXISTS idleRegion LIKE movesdb20241112.idleRegion;
INSERT INTO idleRegion SELECT * FROM movesdb20241112.idleRegion;

CREATE TABLE IF NOT EXISTS totalIdleFraction LIKE movesdb20241112.totalIdleFraction;
INSERT INTO totalIdleFraction SELECT * FROM movesdb20241112.totalIdleFraction WHERE monthID=7;

-- ===========================================================================
-- I/M, retrofit
-- ===========================================================================
CREATE TABLE IF NOT EXISTS IMCoverage LIKE movesdb20241112.IMCoverage;
INSERT INTO IMCoverage SELECT * FROM movesdb20241112.IMCoverage WHERE countyID=26161 AND yearID=2020;

CREATE TABLE IF NOT EXISTS onRoadRetrofit LIKE movesdb20241112.onRoadRetrofit;
INSERT INTO onRoadRetrofit SELECT * FROM movesdb20241112.onRoadRetrofit;

-- ===========================================================================
-- Starts
-- ===========================================================================
CREATE TABLE IF NOT EXISTS Starts LIKE movesdb20241112.Starts;
INSERT INTO Starts SELECT * FROM movesdb20241112.Starts WHERE zoneID=261610 AND yearID=2020 AND monthID=7;

CREATE TABLE IF NOT EXISTS startsAgeAdjustment LIKE movesdb20241112.startsAgeAdjustment;
INSERT INTO startsAgeAdjustment SELECT * FROM movesdb20241112.startsAgeAdjustment;

CREATE TABLE IF NOT EXISTS startsHourFraction LIKE movesdb20241112.startsHourFraction;
INSERT INTO startsHourFraction SELECT * FROM movesdb20241112.startsHourFraction;

CREATE TABLE IF NOT EXISTS startsMonthAdjust LIKE movesdb20241112.startsMonthAdjust;
INSERT INTO startsMonthAdjust SELECT * FROM movesdb20241112.startsMonthAdjust WHERE monthID=7;

CREATE TABLE IF NOT EXISTS startsOpModeDistribution LIKE movesdb20241112.startsOpModeDistribution;
INSERT INTO startsOpModeDistribution SELECT * FROM movesdb20241112.startsOpModeDistribution;

CREATE TABLE IF NOT EXISTS startsPerDay LIKE movesdb20241112.startsPerDay;
INSERT INTO startsPerDay SELECT * FROM movesdb20241112.startsPerDay;

CREATE TABLE IF NOT EXISTS startsPerDayPerVehicle LIKE movesdb20241112.startsPerDayPerVehicle;
INSERT INTO startsPerDayPerVehicle SELECT * FROM movesdb20241112.startsPerDayPerVehicle;

CREATE TABLE IF NOT EXISTS importStartsOpModeDistribution LIKE movesdb20241112.importStartsOpModeDistribution;
INSERT INTO importStartsOpModeDistribution SELECT * FROM movesdb20241112.importStartsOpModeDistribution;

CREATE TABLE IF NOT EXISTS startsSourceTypeFraction LIKE movesdb20241112.startsSourceTypeFraction;
INSERT INTO startsSourceTypeFraction SELECT * FROM movesdb20241112.startsSourceTypeFraction;

-- ===========================================================================
-- VMT (the project importer validates these tables even though PROJECT
-- activity comes from linkSourceTypeHour, not VMT)
-- ===========================================================================
CREATE TABLE IF NOT EXISTS HPMSVtypeDay LIKE movesdb20241112.HPMSVtypeDay;
INSERT INTO HPMSVtypeDay SELECT * FROM movesdb20241112.HPMSVtypeDay WHERE yearID=2020 AND monthID=7;

CREATE TABLE IF NOT EXISTS HPMSVtypeYear LIKE movesdb20241112.HPMSVtypeYear;
INSERT INTO HPMSVtypeYear SELECT * FROM movesdb20241112.HPMSVtypeYear WHERE yearID=2020;

CREATE TABLE IF NOT EXISTS MonthVMTFraction LIKE movesdb20241112.MonthVMTFraction;
INSERT INTO MonthVMTFraction SELECT * FROM movesdb20241112.MonthVMTFraction WHERE monthID=7;

CREATE TABLE IF NOT EXISTS DayVMTFraction LIKE movesdb20241112.DayVMTFraction;
INSERT INTO DayVMTFraction SELECT * FROM movesdb20241112.DayVMTFraction WHERE monthID=7;

CREATE TABLE IF NOT EXISTS HourVMTFraction LIKE movesdb20241112.HourVMTFraction;
INSERT INTO HourVMTFraction SELECT * FROM movesdb20241112.HourVMTFraction;

CREATE TABLE IF NOT EXISTS SourceTypeDayVMT LIKE movesdb20241112.SourceTypeDayVMT;
INSERT INTO SourceTypeDayVMT SELECT * FROM movesdb20241112.SourceTypeDayVMT WHERE yearID=2020 AND monthID=7;

CREATE TABLE IF NOT EXISTS SourceTypeYearVMT LIKE movesdb20241112.SourceTypeYearVMT;
INSERT INTO SourceTypeYearVMT SELECT * FROM movesdb20241112.SourceTypeYearVMT WHERE yearID=2020;

-- ===========================================================================
-- The project network — the three invented rows
-- ===========================================================================
-- INVENTED ROW 1 of 3 — `link`.
-- Must be invented: the default database has no `link` rows at all (a link
-- is a user's roadway segment; EPA cannot ship one). A PROJECT run with an
-- empty `link` table has nothing to simulate.
-- Represents: one 1.0-mile urban restricted-access (roadTypeID=4) segment in
-- zone 261610, carrying 1000 veh/h at 30 mph on level grade. The average
-- speed is the only value the operating-mode result is sensitive to: it is
-- what LinkOperatingModeDistributionGenerator converts into a drive-schedule
-- blend and thence into the opModeDistribution this fixture exists to pin.
CREATE TABLE IF NOT EXISTS Link LIKE movesdb20241112.Link;
INSERT INTO Link (linkID, countyID, zoneID, roadTypeID,
                  linkLength, linkVolume, linkAvgSpeed, linkDescription, linkAvgGrade)
VALUES (1, 26161, 261610, 4, 1.0, 1000.0, 30.0, 'scale-project single test link', 0.0);

-- INVENTED ROW 2 of 3 — `linkSourceTypeHour`.
-- Must be invented: it is keyed by linkID, so it can only be written once a
-- link exists, and no default row can reference invented link 1.
-- Represents: the whole of link 1's volume is source type 21 (passenger
-- car), the only source type the RunSpec selects. Fraction 1.0 means the
-- fixture's emissions are attributable to one source type, which keeps the
-- opModeDistribution readable as a single distribution.
CREATE TABLE IF NOT EXISTS linkSourceTypeHour LIKE movesdb20241112.linkSourceTypeHour;
INSERT INTO linkSourceTypeHour (linkID, sourceTypeID, sourceTypeHourFraction)
VALUES (1, 21, 1.0);

-- INVENTED ROW 3 of 3 — `offNetworkLink`.
-- Must be invented: ProjectTAG.makeStarts and makeSHP read vehicle
-- population, start activity and parked-vehicle activity EXCLUSIVELY from
-- offNetworkLink in PROJECT domain; there is no default-database fallback,
-- and with the table empty the Start Exhaust half of this fixture's
-- pollutant-process set emits nothing.
-- Represents: 1000 passenger cars parked off-network in zone 261610, 5% of
-- which start in the modelled hour and 50% of which are parked. No extended
-- idling (0.0), consistent with a passenger-car-only fixture.
CREATE TABLE IF NOT EXISTS offNetworkLink LIKE movesdb20241112.offNetworkLink;
INSERT INTO offNetworkLink (sourceTypeID, vehiclePopulation, startFraction,
                            extendedIdleFraction, parkedVehicleFraction, zoneID)
VALUES (21, 1000.0, 0.05, 0.0, 0.5, 261610);

-- ---------------------------------------------------------------------------
-- Left EMPTY on purpose. These two are the point of the fixture.
--
-- `opModeDistribution` empty  => no user-supplied distribution, so
--   LinkOperatingModeDistributionGenerator computes one and writes it back
--   with isUserInput='N'. Seeding it here would make the generator a no-op
--   and the snapshot would pin the input instead of the model.
-- `driveScheduleSecondLink` empty => no user-supplied second-by-second drive
--   schedule, so the generator falls through to the average-speed path
--   (linkAvgSpeed above) rather than the trajectory path.
-- Both tables must still EXIST: the project importer errors on an absent
-- table, and the generator's own queries select from them.
CREATE TABLE IF NOT EXISTS OpModeDistribution LIKE movesdb20241112.OpModeDistribution;
CREATE TABLE IF NOT EXISTS driveScheduleSecondLink LIKE movesdb20241112.driveScheduleSecondLink;
