PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE sample (
	id TEXT PRIMARY KEY,
	genome TEXT NOT NULL,
	assembly TEXT NOT NULL,
	platform TEXT NOT NULL,
	name TEXT NOT NULL,
	reads INTEGER NOT NULL);

CREATE TABLE bins (
	id INTEGER PRIMARY KEY,
	size INTEGER NOT NULL UNIQUE,
	reads INTEGER NOT NULL,
	bpm_scale_factor REAL NOT NULL);

CREATE TABLE chromosomes (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE reads (
	chr_id INTEGER NOT NULL,
	bin INTEGER NOT NULL,
	start INTEGER NOT NULL,
	end INTEGER NOT NULL,
	count INTEGER NOT NULL,
	PRIMARY KEY (chr_id, bin, start),
	FOREIGN KEY(chr_id) REFERENCES chromosomes(id),
	FOREIGN KEY(bin) REFERENCES bins(size) ON DELETE CASCADE);

-- CREATE INDEX reads_chr_bin_start_end_idx ON reads(chr, bin, start, end);