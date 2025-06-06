PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE datasets (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL,
	genome TEXT NOT NULL,
	platform TEXT NOT NULL,
	name TEXT NOT NULL,
	tags TEXT,
	description TEXT,
	UNIQUE(genome, platform, dataset, name));
CREATE INDEX datasets_idx ON datasets(platform, genome, name);
CREATE INDEX dataset_public_id_idx ON datasets(public_id);
CREATE INDEX dataset_tags_idx ON datasets(tags);

CREATE TABLE tracks (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL,
	genome TEXT NOT NULL,
	platform TEXT NOT NULL,
	dataset TEXT NOT NULL,
	name TEXT NOT NULL,
	reads INTEGER NOT NULL,
	track_type TEXT NOT NULL,
	url TEXT NOT NULL,
	tags TEXT,
	description TEXT,
	UNIQUE(genome, platform, dataset, name));
CREATE INDEX tracks_idx ON tracks(platform, genome, name);
CREATE INDEX tracks_public_id_idx ON tracks(public_id);
CREATE INDEX tracks_tags_idx ON tracks(tags);