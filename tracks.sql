PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE tracks (
	id INTEGER PRIMARY KEY ASC,
	platform TEXT NOT NULL,
	genome TEXT NOT NULL,
	name TEXT NOT NULL,
	reads INTEGER NOT NULL,
	stat_mode TEXT NOT NULL,
	dir TEXT NOT NULL,
	UNIQUE(id, platform, genome, name));
CREATE INDEX tracks_idx ON tracks(platform, genome, name);