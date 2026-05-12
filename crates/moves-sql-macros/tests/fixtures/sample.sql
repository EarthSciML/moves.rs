-- Synthetic MOVES-style SQL fixture exercising the macro expander and
-- section-marker preprocessor end-to-end. The shape is modelled on
-- ActivityCalculator.sql + BaseRateCalculator.sql but reduced to the
-- minimum that exercises every interesting code path.

-- Section Create Remote Tables for Extracted Data
CREATE TABLE foo (id INT, year INT);
-- Section Inventory
INSERT INTO foo VALUES (1, ##context.year##);
-- End Section Inventory
-- Section Rates
INSERT INTO foo VALUES (2, ##context.year##);
-- End Section Rates
DROP TABLE bar##macro.sourceTypeID##;
-- End Section Create Remote Tables for Extracted Data

-- Section Extract Data
SELECT * FROM zonemonthhour WHERE hourID in (##macro.csv.all.hourID##);
-- End Section Extract Data

-- Section Cleanup
DELETE FROM scratch WHERE fuelTypeID in (##macro.csv.all.fuelTypeID##);
-- End Section Cleanup
