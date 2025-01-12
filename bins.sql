PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE track (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL,
	platform TEXT NOT NULL,
	genome TEXT NOT NULL,
	name TEXT NOT NULL,
	chr TEXT NOT NULL,
	reads INTEGER NOT NULL);
	
-- CREATE TABLE track (
	-- id INTEGER PRIMARY KEY ASC,
	-- bin INTEGER NOT NULL UNIQUE,
	-- reads INTEGER NOT NULL,
	-- UNIQUE(bin, reads));
-- CREATE INDEX track_bin_idx ON track (bin);

CREATE TABLE bpm_scale_factors (
	bin_size INTEGER PRIMARY KEY,
	scale_factor REAL NOT NULL);

CREATE TABLE bins50 (
	start INTEGER PRIMARY KEY,
	end INTEGER NOT NULL,
	reads INTEGER NOT NULL);

CREATE TABLE bins500 (
	start INTEGER PRIMARY KEY,
	end INTEGER NOT NULL,
	reads INTEGER NOT NULL);

CREATE TABLE bins5000 (
        start INTEGER PRIMARY KEY,
        end INTEGER NOT NULL,
        reads INTEGER NOT NULL);
