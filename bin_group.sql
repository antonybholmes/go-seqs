PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE track (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL,
	platform TEXT NOT NULL,
	genome TEXT NOT NULL,
	name TEXT NOT NULL,
	bin_width INTEGER NOT NULL,
	reads INTEGER NOT NULL,
	bpm_scaling_factor REAL NOT NULL);