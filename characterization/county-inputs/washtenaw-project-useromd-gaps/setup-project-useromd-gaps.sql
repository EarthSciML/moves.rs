-- setup-project-useromd-gaps.sql — PROJECT-domain scale input database for
-- Washtenaw County (FIPS 26161, zone 261610, AUGUST 2020, hour 8, day 5)
-- whose user states operating modes that leave GAPS, and brake and tire wear
-- modes.
--
-- Serves fixture: characterization/fixtures/project-useromd-gaps.xml
-- Loaded into database `washtenaw_cdb` by
--   apptainer/capture-county-snapshot.sh --fixture project-useromd-gaps \
--       --county-sql characterization/county-inputs/washtenaw-project-useromd-gaps/setup-project-useromd-gaps.sql
--
-- Everything above "The project network" is washtenaw-project-links/setup-project-links.sql's,
-- copied unchanged.
--
-- WHY THIS FIXTURE EXISTS
-- -----------------------
-- project-useromd states a link's running distribution for every running
-- pollutant-process and source type the run selects, and no brake or tire
-- rows. Here, on link 1, the user states running CO (201) but not running
-- Total Energy (9101), brake wear (11609) for passenger cars but not light
-- commercial trucks, and tire wear (11710) for passenger cars. Link 1 has a
-- running row, so LinkOperatingModeDistributionGenerator skips it (java:330):
-- what MOVES weights Total Energy and the trucks' brake wear by there is
-- whatever else is in opModeDistribution -- nothing. Tire wear's project
-- generator writes RatesOpModeDistribution from the link's speed alone and
-- never reads opModeDistribution (AverageSpeedOperatingModeDistributionGenerator
-- java:146-190). On link 2 the user states brake wear only: the generator
-- runs there, and its tempExistingOpMode anti-join (java:836-860) keeps the
-- user's brake rows over its own.

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
INSERT INTO hotellingMonthAdjust SELECT * FROM movesdb20241112.hotellingMonthAdjust WHERE zoneID=261610 AND monthID=8;

CREATE TABLE IF NOT EXISTS hotellingHours LIKE movesdb20241112.hotellingHours;
INSERT INTO hotellingHours SELECT * FROM movesdb20241112.hotellingHours WHERE zoneID=261610 AND yearID=2020 AND monthID=8;

CREATE TABLE IF NOT EXISTS idleDayAdjust LIKE movesdb20241112.idleDayAdjust;
INSERT INTO idleDayAdjust SELECT * FROM movesdb20241112.idleDayAdjust;

CREATE TABLE IF NOT EXISTS idleModelYearGrouping LIKE movesdb20241112.idleModelYearGrouping;
INSERT INTO idleModelYearGrouping SELECT * FROM movesdb20241112.idleModelYearGrouping;

CREATE TABLE IF NOT EXISTS idleMonthAdjust LIKE movesdb20241112.idleMonthAdjust;
INSERT INTO idleMonthAdjust SELECT * FROM movesdb20241112.idleMonthAdjust WHERE monthID=8;

CREATE TABLE IF NOT EXISTS idleRegion LIKE movesdb20241112.idleRegion;
INSERT INTO idleRegion SELECT * FROM movesdb20241112.idleRegion;

CREATE TABLE IF NOT EXISTS totalIdleFraction LIKE movesdb20241112.totalIdleFraction;
INSERT INTO totalIdleFraction SELECT * FROM movesdb20241112.totalIdleFraction WHERE monthID=8;

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
INSERT INTO Starts SELECT * FROM movesdb20241112.Starts WHERE zoneID=261610 AND yearID=2020 AND monthID=8;

CREATE TABLE IF NOT EXISTS startsAgeAdjustment LIKE movesdb20241112.startsAgeAdjustment;
INSERT INTO startsAgeAdjustment SELECT * FROM movesdb20241112.startsAgeAdjustment;

CREATE TABLE IF NOT EXISTS startsHourFraction LIKE movesdb20241112.startsHourFraction;
INSERT INTO startsHourFraction SELECT * FROM movesdb20241112.startsHourFraction;

CREATE TABLE IF NOT EXISTS startsMonthAdjust LIKE movesdb20241112.startsMonthAdjust;
INSERT INTO startsMonthAdjust SELECT * FROM movesdb20241112.startsMonthAdjust WHERE monthID=8;

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
INSERT INTO HPMSVtypeDay SELECT * FROM movesdb20241112.HPMSVtypeDay WHERE yearID=2020 AND monthID=8;

CREATE TABLE IF NOT EXISTS HPMSVtypeYear LIKE movesdb20241112.HPMSVtypeYear;
INSERT INTO HPMSVtypeYear SELECT * FROM movesdb20241112.HPMSVtypeYear WHERE yearID=2020;

CREATE TABLE IF NOT EXISTS MonthVMTFraction LIKE movesdb20241112.MonthVMTFraction;
INSERT INTO MonthVMTFraction SELECT * FROM movesdb20241112.MonthVMTFraction WHERE monthID=8;

CREATE TABLE IF NOT EXISTS DayVMTFraction LIKE movesdb20241112.DayVMTFraction;
INSERT INTO DayVMTFraction SELECT * FROM movesdb20241112.DayVMTFraction WHERE monthID=8;

CREATE TABLE IF NOT EXISTS HourVMTFraction LIKE movesdb20241112.HourVMTFraction;
INSERT INTO HourVMTFraction SELECT * FROM movesdb20241112.HourVMTFraction;

CREATE TABLE IF NOT EXISTS SourceTypeDayVMT LIKE movesdb20241112.SourceTypeDayVMT;
INSERT INTO SourceTypeDayVMT SELECT * FROM movesdb20241112.SourceTypeDayVMT WHERE yearID=2020 AND monthID=8;

CREATE TABLE IF NOT EXISTS SourceTypeYearVMT LIKE movesdb20241112.SourceTypeYearVMT;
INSERT INTO SourceTypeYearVMT SELECT * FROM movesdb20241112.SourceTypeYearVMT WHERE yearID=2020;

-- ===========================================================================
-- The project network — the invented rows
-- ===========================================================================
CREATE TABLE IF NOT EXISTS Link LIKE movesdb20241112.Link;
INSERT INTO Link (linkID, countyID, zoneID, roadTypeID,
                  linkLength, linkVolume, linkAvgSpeed, linkDescription, linkAvgGrade)
VALUES (1, 26161, 261610, 4, 1.0, 1000.0, 30.0, 'project-useromd-gaps: restricted, user modes with gaps', 0.0),
       (2, 26161, 261610, 5, 0.5,  600.0, 25.0, 'project-useromd-gaps: unrestricted, user brake modes', 0.0);

CREATE TABLE IF NOT EXISTS linkSourceTypeHour LIKE movesdb20241112.linkSourceTypeHour;
INSERT INTO linkSourceTypeHour (linkID, sourceTypeID, sourceTypeHourFraction)
VALUES (1, 21, 0.8), (1, 32, 0.2),
       (2, 21, 0.6), (2, 32, 0.4);

CREATE TABLE IF NOT EXISTS offNetworkLink LIKE movesdb20241112.offNetworkLink;
INSERT INTO offNetworkLink (sourceTypeID, vehiclePopulation, startFraction,
                            extendedIdleFraction, parkedVehicleFraction, zoneID)
VALUES (21, 1000.0, 0.05, 0.0, 0.5, 261610),
       (32,  400.0, 0.08, 0.0, 0.6, 261610);

-- INVENTED ROWS. Link 1: running CO (201) for both source types over
-- thirteen modes, as project-useromd; brake wear (11609) for source type 21
-- over modes 0, 1, 501 and 11; tire wear (11710) for source type 21 at mode
-- 416, which the link's 30 mph would not give (407). Link 2: brake wear for
-- both source types over modes 0, 1 and 501.
CREATE TABLE IF NOT EXISTS OpModeDistribution LIKE movesdb20241112.OpModeDistribution;
INSERT INTO OpModeDistribution (sourceTypeID, hourDayID, linkID, polProcessID,
                                opModeID, opModeFraction, opModeFractionCV, isUserInput)
VALUES
       (21, 95, 1, 201, 0, 0.05, NULL, 'Y'),
       (21, 95, 1, 201, 1, 0.10, NULL, 'Y'),
       (21, 95, 1, 201, 11, 0.10, NULL, 'Y'),
       (21, 95, 1, 201, 21, 0.15, NULL, 'Y'),
       (21, 95, 1, 201, 22, 0.10, NULL, 'Y'),
       (21, 95, 1, 201, 23, 0.10, NULL, 'Y'),
       (21, 95, 1, 201, 24, 0.10, NULL, 'Y'),
       (21, 95, 1, 201, 25, 0.05, NULL, 'Y'),
       (21, 95, 1, 201, 27, 0.05, NULL, 'Y'),
       (21, 95, 1, 201, 28, 0.05, NULL, 'Y'),
       (21, 95, 1, 201, 29, 0.05, NULL, 'Y'),
       (21, 95, 1, 201, 30, 0.05, NULL, 'Y'),
       (21, 95, 1, 201, 33, 0.05, NULL, 'Y'),
       (32, 95, 1, 201, 0, 0.05, NULL, 'Y'),
       (32, 95, 1, 201, 1, 0.10, NULL, 'Y'),
       (32, 95, 1, 201, 11, 0.10, NULL, 'Y'),
       (32, 95, 1, 201, 21, 0.15, NULL, 'Y'),
       (32, 95, 1, 201, 22, 0.10, NULL, 'Y'),
       (32, 95, 1, 201, 23, 0.10, NULL, 'Y'),
       (32, 95, 1, 201, 24, 0.10, NULL, 'Y'),
       (32, 95, 1, 201, 25, 0.05, NULL, 'Y'),
       (32, 95, 1, 201, 27, 0.05, NULL, 'Y'),
       (32, 95, 1, 201, 28, 0.05, NULL, 'Y'),
       (32, 95, 1, 201, 29, 0.05, NULL, 'Y'),
       (32, 95, 1, 201, 30, 0.05, NULL, 'Y'),
       (32, 95, 1, 201, 33, 0.05, NULL, 'Y'),
       (21, 95, 1, 11609, 0, 0.20, NULL, 'Y'),
       (21, 95, 1, 11609, 1, 0.10, NULL, 'Y'),
       (21, 95, 1, 11609, 501, 0.30, NULL, 'Y'),
       (21, 95, 1, 11609, 11, 0.40, NULL, 'Y'),
       (21, 95, 1, 11710, 416, 1.00, NULL, 'Y'),
       (21, 95, 2, 11609, 0, 0.30, NULL, 'Y'),
       (21, 95, 2, 11609, 1, 0.20, NULL, 'Y'),
       (21, 95, 2, 11609, 501, 0.50, NULL, 'Y'),
       (32, 95, 2, 11609, 0, 0.15, NULL, 'Y'),
       (32, 95, 2, 11609, 1, 0.25, NULL, 'Y'),
       (32, 95, 2, 11609, 501, 0.60, NULL, 'Y');

CREATE TABLE IF NOT EXISTS driveScheduleSecondLink LIKE movesdb20241112.driveScheduleSecondLink;
