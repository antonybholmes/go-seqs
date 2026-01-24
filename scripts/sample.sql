PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE sample (
	id TEXT PRIMARY KEY,
	genome TEXT NOT NULL,
	assembly TEXT NOT NULL,
	platform TEXT NOT NULL,
	name TEXT NOT NULL,
	reads INTEGER NOT NULL);