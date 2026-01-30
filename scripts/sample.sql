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
	chr TEXT NOT NULL,
	size INTEGER PRIMARY KEY ASC,
	bpm INTEGER NOT NULL,
	bpm_scale_factor REAL NOT NULL,
	UNIQUE(size, bpm, bpm_scale_factor));

CREATE TABLE reads (
	id INTEGER PRIMARY KEY ASC,
	chr TEXT NOT NULL,
	bin INTEGER NOT NULL,
	start INTEGER NOT NULL,
	end INTEGER NOT NULL,
	count INTEGER NOT NULL,
	UNIQUE(chr, bin, start, end, count),
	FOREIGN KEY(bin) REFERENCES bins(size) ON DELETE CASCADE);

CREATE INDEX reads_chr_bin_start_end_idx ON reads(chr, bin, start, end);