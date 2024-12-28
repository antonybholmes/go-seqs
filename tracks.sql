PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE tracks (
	id INTEGER PRIMARY KEY ASC,
	platform TEXT NOT NULL,
	genome TEXT NOT NULL,
	public_id TEXT NOT NULL,
	name TEXT NOT NULL,
	reads INTEGER NOT NULL,
	stat_mode TEXT NOT NULL,
	dir TEXT NOT NULL,
	tags TEXT,
	description TEXT,
	UNIQUE(platform, genome, public_id, name));
CREATE INDEX tracks_idx ON tracks(platform, genome, name);
CREATE INDEX tracks_public_id_idx ON tracks(public_id);
CREATE INDEX tracks_tags_idx ON tracks(tags);