PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE datasets (
	id TEXT PRIMARY KEY,
	genome TEXT NOT NULL,
	assembly TEXT NOT NULL,
	platform TEXT NOT NULL,
	name TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '',
	UNIQUE(genome, assembly, platform, name));
CREATE INDEX datasets_idx ON datasets(genome, assembly, platform, name);
CREATE INDEX dataset_tags_idx ON datasets(tags);

CREATE TABLE permissions (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL);
CREATE INDEX permissions_name_idx ON permissions(name);

CREATE TABLE dataset_permissions (
	dataset_id TEXT,
    permission_id TEXT,
    PRIMARY KEY(dataset_id, permission_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id),
    FOREIGN KEY (permission_id) REFERENCES permissions(id));

CREATE TABLE samples (
	id TEXT PRIMARY KEY,
	dataset_id TEXT NOT NULL,
	name TEXT NOT NULL,
	reads INTEGER NOT NULL,
	type TEXT NOT NULL,
	url TEXT NOT NULL DEFAULT '',
	description TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '',
	UNIQUE(dataset_id, name),
	FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE);
CREATE INDEX samples_name_idx ON samples(name);
CREATE INDEX samples_tags_idx ON samples(tags);


INSERT INTO permissions (id, name) VALUES ('019bebfc-30dc-7569-8727-02c741227ad8', 'rdf:view');

