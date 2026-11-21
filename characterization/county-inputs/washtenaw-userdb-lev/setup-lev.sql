-- setup-lev.sql -- a LEV input database, built EXACTLY as MOVES 5's LEV tool builds
-- it, for the LEV capture of moves.esm PLAN.md §7.3.82.
--
-- Serves fixture:
--   characterization/fixtures/userdb-lev.xml (DEFAULT domain; applied ALONE, as the
--   user input database the RunSpec names in <databaseselections>)
--
-- WHAT THIS IS. MOVES 5 has no run-time LEV strategy. Tools > "Build LEV Input
-- Database" (gui/BuildLEVNLEV.java, BuildLEVAction.java) runs a COPY of
-- database/LEV_NLEVScripts/MOVES5_MyLEVS_Template.sql that the user has edited --
-- deleting the model-year-group lines NOT in their state's LEV program -- with
-- ##outputdb## and ##defaultdb## substituted, and the user then adds the database
-- to the RunSpec. Below the dashed line is the 11/7/2024 template, byte for byte
-- (as /opt/moves/database/LEV_NLEVScripts/ in canonical-moves.sif carries it),
-- with exactly those three edits:
--   ##outputdb##  -> washtenaw_cdb (the name capture-county-snapshot.sh seeds)
--   ##defaultdb## -> movesdb20241112
--   model-year groups KEPT: 99, 20-23 (1999-2003) and 30-37 (2010-2017);
--   DELETED: 94-98 (1994-1998), 24-29 (2004-2009), 38-45 (2018-2025).
-- So a 2020 run has cohorts at LEV rates (1999-2003, 2010-2017) and cohorts at
-- default rates on both sides of each kept band: 1990-1993 (before any LEV
-- program), 1994-1998 and 2004-2009 (LEV years the user did not keep) and
-- 2018-2020 (kept nowhere). emissionRateByAgeLEV has no group-37 rows for
-- regulatory class 20 and half of them for class 30, so 2017 is LEV only where
-- the default LEV table has a row.
--
-- The database holds ONE table. A DEFAULT-domain run does not read a domain
-- database, and userdb-lev.xml leaves <scaleinputdatabase> empty (its description
-- is not the placeholder capture-county-snapshot.sh fills in), so MOVESEngine's
-- hotelling-table check -- which reads the domain database only -- does not apply.
-- ---------------------------------------------------------------------------
-- ###################################################################
-- Template name: MOVES5_MyLEVs_Template.sql
-- Template date: 11/7/2024
-- 
-- This is a template script to be used in conjunction with the LEV
-- Tool in MOVES. To access the tool, open the Tools menu in MOVES, 
-- and select "Build LEV Input Database". The full instructions for
-- using this tool can be found by clicking the "Open Instructions"
-- button at the bottom of the tool's screen.

-- This tool creates a new MOVES input database that contains adjusted
-- LEV emission rates for a subset of model years.

-- To use this tool, save a new copy of this template script, and note
-- the save location and new file name. Then, edit the final command
-- in this script, as described in detail below. Save the file.
-- Back in the tool in the MOVES user interface, select your new file
-- as the "Builder Script", and enter a database name for the new
-- database. After clicking "Build Database", click "Done", then
-- navigate to the Advanced Features panel. In the upper right corner
-- of this screen, select your database in the "Input Data Sets"
-- drop-down menu (note: you may need to click the "Refresh" button),
-- and click the "Add" button. After performing these steps, when
-- MOVES runs, it will use the correct emission rates.
-- 
-- Note: This template script cannot be run directly outside of the 
-- MOVES user interface. If you would like to execute it manually 
-- (for example, in HeidiSQL), you will need to replace
-- "washtenaw_cdb" with your target new database name, as well as 
-- "movesdb20241112" with the name of the default MOVES database.
-- ###################################################################

-- Create database
drop database if exists washtenaw_cdb;
create database washtenaw_cdb;

-- Create a table to hold the adjusted LEV emission rates.
drop table if exists washtenaw_cdb.emissionratebyage;
CREATE TABLE washtenaw_cdb.emissionratebyage (
  `sourceBinID` bigint(20) NOT NULL default '0',
  `polProcessID` smallint(6) NOT NULL default '0',
  `opModeID` smallint(6) NOT NULL default '0',
  `ageGroupID` smallint(6) NOT NULL default '0',
  `meanBaseRate` float default NULL,
  `meanBaseRateCV` float default NULL,
  `meanBaseRateIM` float default NULL,
  `meanBaseRateIMCV` float default NULL,
  `dataSourceId` smallint(6) default NULL,
  PRIMARY KEY  (`ageGroupID`,`opModeID`,`polProcessID`,`sourceBinID`),
  UNIQUE KEY `XPKEmissionRateByAge` 
    (`sourceBinID`,`polProcessID`,`opModeID`,`ageGroupID`),
  KEY `polProcessID` (`polProcessID`)
) ENGINE=MyISAM DEFAULT CHARSET='utf8mb4' COLLATE 'utf8mb4_unicode_ci';

-- ###################################################################

-- The next command selects LEV emission rates for model years covered
-- by the LEV program. In the sequence of short codes at the end of 
-- this command, keep the lines that correspond to model years in the 
-- LEV program and delete the lines that correspond to model years NOT 
-- in the LEV program.
--
-- Note:
-- Model years 1994-2003 are LEV I program model years.
-- Model years 2004-2014 are LEV II program model years. 
-- Model years 2015-2025 are LEV III program model years. 

-- ###################################################################

insert into washtenaw_cdb.emissionratebyage (
	sourcebinid,
	polprocessid,
	opmodeid,
	agegroupid,
	meanbaserate,
	meanbaseratecv,
	meanbaserateim,
	meanbaserateimcv,
	datasourceid )
select  
	sourcebinid,
	polprocessid,
	opmodeid,
	agegroupid,
	meanbaserate,
	meanbaseratecv,
	meanbaserateim,
	meanbaserateimcv,
	datasourceid 
from movesdb20241112.emissionratebyagelev
where polprocessid in (101,201,301,102,202,302,11201,11202,11801,11802)
and mid(sourcebinid,8,2) in (''
	-- shortModYrGroupID | modelYearGroupName 
   ,99 			   -- 99 = 1999               
   ,20 			   -- 20 = 2000               
   ,21 			   -- 21 = 2001               
   ,22 			   -- 22 = 2002               
   ,23 			   -- 23 = 2003               
   ,30 			   -- 30 = 2010               
   ,31 			   -- 31 = 2011               
   ,32 			   -- 32 = 2012               
   ,33 			   -- 33 = 2013               
   ,34 			   -- 34 = 2014               
   ,35 			   -- 35 = 2015               
   ,36 			   -- 36 = 2016 
   ,37 			   -- 37 = 2017 
);
