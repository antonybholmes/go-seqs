PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE info (
	id INTEGER PRIMARY KEY ASC,
	genome TEXT NOT NULL,
	chr TEXT NOT NULL,
	bin_width INTEGER NOT NULL);

-- CREATE TABLE track (
	-- id INTEGER PRIMARY KEY ASC,
	-- bin INTEGER NOT NULL UNIQUE,
	-- reads INTEGER NOT NULL,
	-- UNIQUE(bin, reads));
-- CREATE INDEX track_bin_idx ON track (bin);

CREATE TABLE track (
	bin_start INTEGER PRIMARY KEY,
	bin_end INTEGER,
	reads INTEGER NOT NULL);
