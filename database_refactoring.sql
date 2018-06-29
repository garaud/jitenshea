----------
-- lyon --
----------

-- daily_transaction table --
--OK

-- timeserie table --
-- number -> id
-- last_update -> timestamp
-- available_bike_stands -> available_stands
ALTER TABLE lyon.timeserie RENAME COLUMN number TO id;
ALTER TABLE lyon.timeserie RENAME COLUMN last_update TO timestamp;
ALTER TABLE lyon.timeserie RENAME COLUMN available_bike_stands TO available_stands;

-- drop columns : bike_stands, availabilitycode, availability, bonus
ALTER TABLE lyon.timeserie DROP COLUMN IF EXISTS bike_stands;
ALTER TABLE lyon.timeserie DROP COLUMN IF EXISTS availabilitycode;
ALTER TABLE lyon.timeserie DROP COLUMN IF EXISTS availability;
ALTER TABLE lyon.timeserie DROP COLUMN IF EXISTS bonus;

-- id: INT -> VARCHAR
ALTER TABLE lyon.timeserie ALTER COLUMN id TYPE VARCHAR;

-- status column refactoring (value replacement)
UPDATE lyon.timeserie SET status = 'open' WHERE status = 'OPEN';
UPDATE lyon.timeserie SET status = 'closed' WHERE status = 'CLOSED';

-- cluster table --
-- station_id: INT -> VARCHAR
ALTER TABLE lyon.clustering ALTER COLUMN station_id TYPE VARCHAR;

-- centroid table --
-- hX -> h0X
ALTER TABLE lyon.centroid RENAME COLUMN h0 TO h00;
ALTER TABLE lyon.centroid RENAME COLUMN h1 TO h01;
ALTER TABLE lyon.centroid RENAME COLUMN h2 TO h02;
ALTER TABLE lyon.centroid RENAME COLUMN h3 TO h03;
ALTER TABLE lyon.centroid RENAME COLUMN h4 TO h04;
ALTER TABLE lyon.centroid RENAME COLUMN h5 TO h05;
ALTER TABLE lyon.centroid RENAME COLUMN h6 TO h06;
ALTER TABLE lyon.centroid RENAME COLUMN h7 TO h07;
ALTER TABLE lyon.centroid RENAME COLUMN h8 TO h08;
ALTER TABLE lyon.centroid RENAME COLUMN h9 TO h09;

--------------
-- bordeaux --
--------------

-- daily_transaction table --
--OK

-- timeserie table --
-- number -> id
-- last_update -> timestamp
-- available_stand -> available_stands
-- available_bike -> available_bikes
-- state -> status
ALTER TABLE bordeaux.timeserie RENAME COLUMN number TO ident;
ALTER TABLE bordeaux.timeserie RENAME COLUMN ts TO timestamp;
ALTER TABLE bordeaux.timeserie RENAME COLUMN available_stand TO available_stands;
ALTER TABLE bordeaux.timeserie RENAME COLUMN available_bike TO available_bikes;
ALTER TABLE bordeaux.timeserie RENAME COLUMN state TO status;

-- drop columns : gid, type, name
ALTER TABLE bordeaux.timeserie DROP COLUMN IF EXISTS gid;
ALTER TABLE bordeaux.timeserie DROP COLUMN IF EXISTS type;
ALTER TABLE bordeaux.timeserie DROP COLUMN IF EXISTS name;

-- id: INT -> VARCHAR
ALTER TABLE bordeaux.timeserie ALTER COLUMN id TYPE VARCHAR;

-- status column refactoring (value replacements)
UPDATE bordeaux.timeserie SET status = 'open' WHERE status = 'CONNECTEE';
UPDATE bordeaux.timeserie SET status = 'closed' WHERE status = 'DECONNECTEE';

-- cluster table --
-- station_id: INT -> VARCHAR
ALTER TABLE bordeaux.clustering ALTER COLUMN station_id TYPE VARCHAR;

-- centroid table --
-- hX -> h0X
ALTER TABLE bordeaux.centroid RENAME COLUMN h0 TO h00;
ALTER TABLE bordeaux.centroid RENAME COLUMN h1 TO h01;
ALTER TABLE bordeaux.centroid RENAME COLUMN h2 TO h02;
ALTER TABLE bordeaux.centroid RENAME COLUMN h3 TO h03;
ALTER TABLE bordeaux.centroid RENAME COLUMN h4 TO h04;
ALTER TABLE bordeaux.centroid RENAME COLUMN h5 TO h05;
ALTER TABLE bordeaux.centroid RENAME COLUMN h6 TO h06;
ALTER TABLE bordeaux.centroid RENAME COLUMN h7 TO h07;
ALTER TABLE bordeaux.centroid RENAME COLUMN h8 TO h08;
ALTER TABLE bordeaux.centroid RENAME COLUMN h9 TO h09;
