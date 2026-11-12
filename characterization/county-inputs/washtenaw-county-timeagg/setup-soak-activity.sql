-- setup-soak-activity.sql -- STATED soakActivityFraction for Washtenaw County
-- (zone 261610), applied AFTER setup-timeagg.sql (run-all-fixtures.sh's
-- county_sql_for lists both; capture-county-snapshot.sh concatenates them).
--
-- Serves fixtures: characterization/fixtures/scale-county-timeagg-soak-day.xml
--   (<aggregateBy key="24-Hour Day"/>, <outputtimestep value="Month"/>)
-- and characterization/fixtures/scale-county-timeagg-soak-month.xml
--   (<aggregateBy key="Month"/>, <outputtimestep value="Month"/>).
-- Both: January and July 2020, both day types, all 24 hours, the gasoline
-- passenger car (21) and light commercial truck (32), road types 1 and 4,
-- THC at evaporative permeation, fuel vapor venting and fuel leaks.
--
-- WHY THESE RUNS
-- --------------
-- TankTemperatureGenerator TTG-6 keeps a user's soakActivityFraction rows for
-- every (sourceTypeID, zoneID, monthID) that has one (SoakActivityFractionKeys,
-- isUserInput = 'Y') and computes the rest. Under time aggregation the user's
-- hourly rows never reach it as stated: PreAggDAY.sql collapses them to
-- `sum(soakActivityFraction x HourWeighting2)` per day type (hourDayID =
-- dayID), and PreAggMONTH.sql those to `sum(x DayWeighting2)` at hourDayID 0,
-- both before any generator runs. EvaporativeEmissionsOperatingModeDistribution-
-- Generator then weights the soak modes 150 and 151 by the collapsed fraction
-- x (1 - fractionOfOperating).
--
-- THE ROWS
-- --------
-- Every hour-day of the default HourDay, modes 150 (hot soak) and 151 (cold
-- soak), summing to 1 per hour as TTG-6's own rows do. The hot-soak share
-- varies with the hour, the day type, the source type and the month, so that
-- each weighting shows:
--     hot = 0.010 + 0.0015 x hourID + 0.004 x [dayID = 2]
--           + 0.002 x [sourceTypeID = 32] + 0.0003 x monthID
-- (0.0115 to 0.0515), cold = 1 - hot.
--   sourceTypeID 21  every month: stated wherever the runs look.
--   sourceTypeID 32  January only: July is TTG-6's own (under aggregation,
--                    mode 151 at exactly 1), so the key test is per month.

CREATE TABLE IF NOT EXISTS soakActivityFraction LIKE movesdb20241112.soakActivityFraction;

INSERT INTO soakActivityFraction
    (sourceTypeID, zoneID, monthID, hourDayID, opModeID,
     soakActivityFraction, soakActivityFractionCV, isUserInput)
SELECT st.sourceTypeID, 261610, m.monthID, hd.hourDayID, 150,
       0.010 + 0.0015 * hd.hourID + 0.004 * (hd.dayID = 2)
             + 0.002 * (st.sourceTypeID = 32) + 0.0003 * m.monthID,
       NULL, 'Y'
FROM movesdb20241112.HourDay hd
CROSS JOIN movesdb20241112.MonthOfAnyYear m
CROSS JOIN (SELECT 21 AS sourceTypeID UNION ALL SELECT 32) st
WHERE st.sourceTypeID = 21 OR m.monthID = 1;

INSERT INTO soakActivityFraction
    (sourceTypeID, zoneID, monthID, hourDayID, opModeID,
     soakActivityFraction, soakActivityFractionCV, isUserInput)
SELECT sourceTypeID, zoneID, monthID, hourDayID, 151,
       1 - soakActivityFraction, NULL, 'Y'
FROM soakActivityFraction
WHERE opModeID = 150;
