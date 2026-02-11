PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE sample (
	id TEXT PRIMARY KEY,
	platform TEXT NOT NULL,
	genome TEXT NOT NULL,
	assembly TEXT NOT NULL,
	name TEXT NOT NULL,
	chr TEXT NOT NULL,
	reads INTEGER NOT NULL);
	
-- CREATE TABLE track (
	-- id INTEGER PRIMARY KEY ASC,
	-- bin INTEGER NOT NULL UNIQUE,
	-- reads INTEGER NOT NULL,
	-- UNIQUE(bin, reads));
-- CREATE INDEX track_bin_idx ON track (bin);

CREATE TABLE bins (
	size INTEGER PRIMARY KEY ASC,
	bpm INTEGER NOT NULL,
	bpm_scale_factor REAL NOT NULL,
	UNIQUE(size, bpm, bpm_scale_factor));

CREATE TABLE reads (
	id INTEGER PRIMARY KEY ASC,
	bin INTEGER NOT NULL,
	start INTEGER KEY,
	end INTEGER NOT NULL,
	count INTEGER NOT NULL,
	UNIQUE(bin, start, end, count),
	FOREIGN KEY(bin) REFERENCES bins(size) ON DELETE CASCADE);

CREATE INDEX reads_bin_start_end_idx ON reads(bin, start, end);

-- CREATE TABLE bins50 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins500 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins5000 (
--         start INTEGER PRIMARY KEY,
--         end INTEGER NOT NULL,
--         reads INTEGER NOT NULL);

-- CREATE TABLE bins20 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins200 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins2000 (
--         start INTEGER PRIMARY KEY,
--         end INTEGER NOT NULL,
--         reads INTEGER NOT NULL);

-- CREATE TABLE bins20000 (
--         start INTEGER PRIMARY KEY,
--         end INTEGER NOT NULL,
--         reads INTEGER NOT NULL);


-- CREATE TABLE bins10 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins100 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins1000 (
--         start INTEGER PRIMARY KEY,
--         end INTEGER NOT NULL,
--         reads INTEGER NOT NULL);

-- CREATE TABLE bins10000 (
--         start INTEGER PRIMARY KEY,
--         end INTEGER NOT NULL,
--         reads INTEGER NOT NULL);

-- CREATE TABLE bins16 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins64 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins256 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins1024 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins4096 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);

-- CREATE TABLE bins16384 (
-- 	start INTEGER PRIMARY KEY,
-- 	end INTEGER NOT NULL,
-- 	reads INTEGER NOT NULL);