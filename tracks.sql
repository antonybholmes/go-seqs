PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE tracks (
	id INTEGER PRIMARY KEY ASC,
	uuid TEXT NOT NULL,
	genome TEXT NOT NULL,
	platform TEXT NOT NULL,
	dataset TEXT NOT NULL,
	name TEXT NOT NULL,
	reads INTEGER NOT NULL,
	stat_mode TEXT NOT NULL,
	dir TEXT NOT NULL,
	tags TEXT,
	description TEXT,
	UNIQUE(platform, genome, dataset, name));
CREATE INDEX tracks_idx ON tracks(platform, genome, name);
CREATE INDEX tracks_uuid_idx ON tracks(uuid);
CREATE INDEX tracks_tags_idx ON tracks(tags);