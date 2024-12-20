PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE info track (
	id INTEGER PRIMARY KEY ASC,
	platform TEXT NOT NULL,
	genome TEXT NOT NULL,
	name TEXT NOT NULL,
	reads INTEGER NOT NULL);