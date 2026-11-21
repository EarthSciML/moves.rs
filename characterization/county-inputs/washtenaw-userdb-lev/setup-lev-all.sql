-- setup-lev-all.sql -- a LEV input database keeping EVERY model-year group, for the
-- LEV capture of moves.esm PLAN.md §7.3.82 that reaches emissionRateByAgeLEV's
-- particulate rows.
--
-- Serves fixture:
--   characterization/fixtures/userdb-lev-2025.xml (DEFAULT domain; applied ALONE, as
--   the user input database the RunSpec names in <databaseselections>)
--
-- Below the dashed line is MOVES5_MyLEVS_Template.sql byte for byte with the LEV
-- tool's two substitutions (##outputdb## washtenaw_cdb, ##defaultdb##
-- movesdb20241112) and NO model-year line deleted: a state in every LEV program
-- year, 1994-2025. emissionRateByAgeLEV holds its elemental-carbon and non-EC PM
-- rows (polProcessIDs 11201, 11202, 11801, 11802) at model-year group 45 (2025)
-- ONLY, so setup-lev.sql's 2020 runs carry none, and only a run whose calendar
-- year reaches model year 2025 reads them.
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
   ,94 			   -- 94 = 1994               
   ,95 			   -- 95 = 1995               
   ,96 			   -- 96 = 1996               
   ,97 			   -- 97 = 1997               
   ,98 			   -- 98 = 1998               
   ,99 			   -- 99 = 1999               
   ,20 			   -- 20 = 2000               
   ,21 			   -- 21 = 2001               
   ,22 			   -- 22 = 2002               
   ,23 			   -- 23 = 2003               
   ,24 			   -- 24 = 2004               
   ,25 			   -- 25 = 2005               
   ,26 			   -- 26 = 2006               
   ,27 			   -- 27 = 2007               
   ,28 			   -- 28 = 2008               
   ,29 			   -- 29 = 2009               
   ,30 			   -- 30 = 2010               
   ,31 			   -- 31 = 2011               
   ,32 			   -- 32 = 2012               
   ,33 			   -- 33 = 2013               
   ,34 			   -- 34 = 2014               
   ,35 			   -- 35 = 2015               
   ,36 			   -- 36 = 2016 
   ,37 			   -- 37 = 2017 
   ,38 			   -- 38 = 2018 
   ,39 			   -- 39 = 2019 
   ,40 			   -- 40 = 2020
   ,41 			   -- 41 = 2021
   ,42 			   -- 42 = 2022
   ,43 			   -- 43 = 2023
   ,44 			   -- 44 = 2024
   ,45 			   -- 45 = 2025
);
