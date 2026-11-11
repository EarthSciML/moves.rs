-- setup-project-driveschedule.sql — PROJECT-domain scale input database for
-- Washtenaw County (FIPS 26161, zone 261610, AUGUST 2020, hour 8, day 5)
-- two of whose on-network links carry a USER DRIVE SCHEDULE.
--
-- Serves fixture: characterization/fixtures/project-driveschedule.xml
-- Loaded into database `washtenaw_cdb` by
--   apptainer/capture-county-snapshot.sh --fixture project-driveschedule \
--       --county-sql characterization/county-inputs/washtenaw-project-driveschedule/setup-project-driveschedule.sql
--
-- Everything above "The project network" is washtenaw-project-links/setup-project-links.sql's,
-- copied unchanged.
--
-- WHY THIS FIXTURE EXISTS
-- -----------------------
-- LinkOperatingModeDistributionGenerator.calculateOpModeFractions (java:309-393)
-- counts a link's driveScheduleSecondLink rows first. A link with any, and with no
-- user running opModeDistribution, is walked second by second through
-- calculateOpModeFractionsCore (java:922-1183) -- the first arm of step 100 -- in
-- place of the two bracketing national schedules, and the core first writes the
-- schedule's average speed and grade back into Link (java:937-948), which ProjectTAG
-- (a lower loop priority, java:71-79) then reads for SHO. Every earlier capture
-- creates the table empty.
--
-- * link 1 (road type 5, 0.3 mi): an arterial approach to a signal -- a
--   deceleration to a stop, 22 s stopped, a creep below 1 mph, acceleration,
--   cruise near 35 mph and a slowdown; level. Its STATED linkAvgSpeed is 30 mph
--   but its schedule averages 21.1 mph, so SHO and tire wear tell which one
--   MOVES uses (30 mph is tire-wear mode 407, 21.1 mph mode 405).
-- * link 2 (road type 4, 0.4 mi): a freeway on-ramp and merge with a per-second
--   grade (4 %, 3 %, 2 %, then 0.5 %), a hard brake in its second second that the
--   uphill grade keeps out of the braking mode, and a three-second logger dropout
--   (seconds 41-43 missing). Stated 45 mph (mode 410); its schedule averages 50.8
--   mph (mode 411).
-- * link 3 (road type 4, 1.0 mi, 55 mph): no schedule -- the bracketing path, in
--   the same run and on the same road type as link 2.

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
VALUES (1, 26161, 261610, 5, 0.3,  700.0, 30.0, 'project-driveschedule: arterial signal approach, user schedule', 0.0),
       (2, 26161, 261610, 4, 0.4,  900.0, 45.0, 'project-driveschedule: freeway on-ramp and merge, user schedule', 3.0),
       (3, 26161, 261610, 4, 1.0, 1500.0, 55.0, 'project-driveschedule: freeway, average speed only', 0.0);

CREATE TABLE IF NOT EXISTS linkSourceTypeHour LIKE movesdb20241112.linkSourceTypeHour;
INSERT INTO linkSourceTypeHour (linkID, sourceTypeID, sourceTypeHourFraction)
VALUES (1, 21, 0.85), (1, 62, 0.15),
       (2, 21, 0.8),  (2, 62, 0.2),
       (3, 21, 0.75), (3, 62, 0.25);

-- `offNetworkLink`: the zone's parked fleet. The run selects no off-network
-- road type; the project importer wants the table.
CREATE TABLE IF NOT EXISTS offNetworkLink LIKE movesdb20241112.offNetworkLink;
INSERT INTO offNetworkLink (sourceTypeID, vehiclePopulation, startFraction,
                            extendedIdleFraction, parkedVehicleFraction, zoneID)
VALUES (21, 1000.0, 0.05, 0.0, 0.5, 261610),
       (62,   50.0, 0.02, 0.3, 0.6, 261610);

-- No user operating modes: links 1 and 2 take the drive-schedule path, link 3 the
-- average-speed path.
CREATE TABLE IF NOT EXISTS OpModeDistribution LIKE movesdb20241112.OpModeDistribution;

-- The user's drive schedules: (linkID, secondID, speed mph, grade percent).
CREATE TABLE IF NOT EXISTS driveScheduleSecondLink LIKE movesdb20241112.driveScheduleSecondLink;
INSERT INTO driveScheduleSecondLink (linkID, secondID, speed, grade)
VALUES
       (1,   1, 32.0, 0.0),
       (1,   2, 31.6, 0.0),
       (1,   3, 30.8, 0.0),
       (1,   4, 29.5, 0.0),
       (1,   5, 27.6, 0.0),
       (1,   6, 25.3, 0.0),
       (1,   7, 22.6, 0.0),
       (1,   8, 19.7, 0.0),
       (1,   9, 16.6, 0.0),
       (1,  10, 13.4, 0.0),
       (1,  11, 10.3, 0.0),
       (1,  12,  7.4, 0.0),
       (1,  13,  4.8, 0.0),
       (1,  14,  2.6, 0.0),
       (1,  15,  1.1, 0.0),
       (1,  16,  0.4, 0.0),
       (1,  17,  0.0, 0.0),
       (1,  18,  0.0, 0.0),
       (1,  19,  0.0, 0.0),
       (1,  20,  0.0, 0.0),
       (1,  21,  0.0, 0.0),
       (1,  22,  0.0, 0.0),
       (1,  23,  0.0, 0.0),
       (1,  24,  0.0, 0.0),
       (1,  25,  0.0, 0.0),
       (1,  26,  0.0, 0.0),
       (1,  27,  0.0, 0.0),
       (1,  28,  0.0, 0.0),
       (1,  29,  0.0, 0.0),
       (1,  30,  0.0, 0.0),
       (1,  31,  0.0, 0.0),
       (1,  32,  0.0, 0.0),
       (1,  33,  0.0, 0.0),
       (1,  34,  0.0, 0.0),
       (1,  35,  0.0, 0.0),
       (1,  36,  0.0, 0.0),
       (1,  37,  0.0, 0.0),
       (1,  38,  0.0, 0.0),
       (1,  39,  0.4, 0.0),
       (1,  40,  0.9, 0.0),
       (1,  41,  2.3, 0.0),
       (1,  42,  4.6, 0.0),
       (1,  43,  7.5, 0.0),
       (1,  44, 10.6, 0.0),
       (1,  45, 13.7, 0.0),
       (1,  46, 16.6, 0.0),
       (1,  47, 19.4, 0.0),
       (1,  48, 22.0, 0.0),
       (1,  49, 24.4, 0.0),
       (1,  50, 26.6, 0.0),
       (1,  51, 28.5, 0.0),
       (1,  52, 30.2, 0.0),
       (1,  53, 31.6, 0.0),
       (1,  54, 32.8, 0.0),
       (1,  55, 33.7, 0.0),
       (1,  56, 34.4, 0.0),
       (1,  57, 34.9, 0.0),
       (1,  58, 35.2, 0.0),
       (1,  59, 35.4, 0.0),
       (1,  60, 35.5, 0.0),
       (1,  61, 35.3, 0.0),
       (1,  62, 35.0, 0.0),
       (1,  63, 34.8, 0.0),
       (1,  64, 34.9, 0.0),
       (1,  65, 35.1, 0.0),
       (1,  66, 35.4, 0.0),
       (1,  67, 35.7, 0.0),
       (1,  68, 35.8, 0.0),
       (1,  69, 35.6, 0.0),
       (1,  70, 35.2, 0.0),
       (1,  71, 34.9, 0.0),
       (1,  72, 34.7, 0.0),
       (1,  73, 34.8, 0.0),
       (1,  74, 35.0, 0.0),
       (1,  75, 35.3, 0.0),
       (1,  76, 35.5, 0.0),
       (1,  77, 35.4, 0.0),
       (1,  78, 35.1, 0.0),
       (1,  79, 34.8, 0.0),
       (1,  80, 34.6, 0.0),
       (1,  81, 34.7, 0.0),
       (1,  82, 35.0, 0.0),
       (1,  83, 35.2, 0.0),
       (1,  84, 35.3, 0.0),
       (1,  85, 35.1, 0.0),
       (1,  86, 34.9, 0.0),
       (1,  87, 34.8, 0.0),
       (1,  88, 34.9, 0.0),
       (1,  89, 34.1, 0.0),
       (1,  90, 32.9, 0.0),
       (1,  91, 31.4, 0.0),
       (1,  92, 29.8, 0.0),
       (1,  93, 28.2, 0.0),
       (1,  94, 26.9, 0.0),
       (1,  95, 25.9, 0.0),
       (1,  96, 25.2, 0.0),
       (1,  97, 24.8, 0.0),
       (1,  98, 24.6, 0.0),
       (1,  99, 24.5, 0.0),
       (1, 100, 24.7, 0.0),
       (1, 101, 25.0, 0.0),
       (1, 102, 25.2, 0.0),
       (1, 103, 25.1, 0.0),
       (1, 104, 24.9, 0.0),
       (1, 105, 24.6, 0.0),
       (1, 106, 24.4, 0.0),
       (1, 107, 24.5, 0.0),
       (1, 108, 24.8, 0.0),
       (1, 109, 25.1, 0.0),
       (1, 110, 25.3, 0.0),
       (2,   1, 27.0, 4.0),
       (2,   2, 24.6, 4.0),
       (2,   3, 23.9, 4.0),
       (2,   4, 24.4, 4.0),
       (2,   5, 25.6, 4.0),
       (2,   6, 27.1, 4.0),
       (2,   7, 28.8, 4.0),
       (2,   8, 30.6, 4.0),
       (2,   9, 32.5, 4.0),
       (2,  10, 34.4, 4.0),
       (2,  11, 36.3, 4.0),
       (2,  12, 38.2, 4.0),
       (2,  13, 40.0, 4.0),
       (2,  14, 41.8, 4.0),
       (2,  15, 43.5, 3.0),
       (2,  16, 45.1, 3.0),
       (2,  17, 46.6, 3.0),
       (2,  18, 48.0, 3.0),
       (2,  19, 49.3, 3.0),
       (2,  20, 50.5, 3.0),
       (2,  21, 51.6, 3.0),
       (2,  22, 52.6, 3.0),
       (2,  23, 53.5, 3.0),
       (2,  24, 54.3, 3.0),
       (2,  25, 55.0, 2.0),
       (2,  26, 55.6, 2.0),
       (2,  27, 56.1, 2.0),
       (2,  28, 56.5, 2.0),
       (2,  29, 56.8, 2.0),
       (2,  30, 57.1, 2.0),
       (2,  31, 57.4, 2.0),
       (2,  32, 57.8, 2.0),
       (2,  33, 58.2, 2.0),
       (2,  34, 58.7, 2.0),
       (2,  35, 59.1, 0.5),
       (2,  36, 59.4, 0.5),
       (2,  37, 59.6, 0.5),
       (2,  38, 59.8, 0.5),
       (2,  39, 60.1, 0.5),
       (2,  40, 60.3, 0.5),
       (2,  44, 60.9, 0.5),
       (2,  45, 61.0, 0.5),
       (2,  46, 61.2, 0.5),
       (2,  47, 61.3, 0.5),
       (2,  48, 61.1, 0.5),
       (2,  49, 60.9, 0.5),
       (2,  50, 60.6, 0.5),
       (2,  51, 60.2, 0.5),
       (2,  52, 59.6, 0.5),
       (2,  53, 58.8, 0.5),
       (2,  54, 57.7, 0.5),
       (2,  55, 56.3, 0.5),
       (2,  56, 54.8, 0.5),
       (2,  57, 53.6, 0.5),
       (2,  58, 52.9, 0.5),
       (2,  59, 52.6, 0.5),
       (2,  60, 52.8, 0.5),
       (2,  61, 53.3, 0.5),
       (2,  62, 54.0, 0.5),
       (2,  63, 54.8, 0.5),
       (2,  64, 55.6, 0.5),
       (2,  65, 56.3, 0.5),
       (2,  66, 56.9, 0.5),
       (2,  67, 57.4, 0.5),
       (2,  68, 57.8, 0.5),
       (2,  69, 58.1, 0.5),
       (2,  70, 58.3, 0.5);
