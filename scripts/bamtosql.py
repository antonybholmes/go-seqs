# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3
import sys

import libseq
import pandas as pd
import uuid_utils as uuid
from nanoid import generate

DIR = "../data/modules/seqs"


parser = argparse.ArgumentParser()

parser.add_argument("-w", "--widths", default="50,100,1000,10000", help="size of bin")
parser.add_argument("-o", "--out", help="output directory")
parser.add_argument("--paired", action="store_true", help="data is paired end")
args = parser.parse_args()

bin_sizes = [int(w) for w in args.widths.split(",")]
outdir = args.out


db = os.path.join(DIR, "seqs.db")


if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f"""
    CREATE TABLE genomes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        scientific_name TEXT NOT NULL,
        UNIQUE(name, scientific_name));
    """,
)

cursor.execute(
    f"INSERT INTO genomes (id, public_id, name, scientific_name) VALUES (1, '{uuid.uuid7()}', 'Human', 'Homo sapiens');"
)
cursor.execute(
    f"INSERT INTO genomes (id, public_id, name, scientific_name) VALUES (2, '{uuid.uuid7()}', 'Mouse', 'Mus musculus');"
)

genome_map = {"Human": 1, "Mouse": 2}

cursor.execute(
    f"""
    CREATE TABLE assemblies (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE);
    """,
)

cursor.execute(
    f"INSERT INTO assemblies (id, public_id, name) VALUES (1, '{uuid.uuid7()}', 'hg19');"
)
cursor.execute(
    f"INSERT INTO assemblies (id, public_id, name) VALUES (2, '{uuid.uuid7()}', 'GRCh38');"
)
cursor.execute(
    f"INSERT INTO assemblies (id, public_id, name) VALUES (3, '{uuid.uuid7()}', 'GRCm39');"
)

assembly_map = {"hg19": 1, "GRCh38": 2, "GRCm39": 3}

cursor.execute(
    f"""
    CREATE TABLE technologies (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE);
    """,
)

cursor.execute(
    f"INSERT INTO technologies (id, public_id, name) VALUES (1, '{uuid.uuid7()}', 'ChIP-seq');"
)
cursor.execute(
    f"INSERT INTO technologies (id, public_id, name) VALUES (2, '{uuid.uuid7()}', 'RNA-seq');"
)
cursor.execute(
    f"INSERT INTO technologies (id, public_id, name) VALUES (3, '{uuid.uuid7()}', 'CUT&RUN');"
)

technology_map = {"ChIP-seq": 1, "RNA-seq": 2, "CUT&RUN": 3}

cursor.execute(
    f""" CREATE TABLE datasets (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	genome_id INTEGER NOT NULL, 
	assembly_id INTEGER NOT NULL,
    technology_id INTEGER NOT NULL,
    name TEXT NOT NULL, 
    description TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(genome_id) REFERENCES genomes(id) ON DELETE CASCADE,
	FOREIGN KEY(assembly_id) REFERENCES assemblies(id) ON DELETE CASCADE,
    FOREIGN KEY(technology_id) REFERENCES technologies(id) ON DELETE CASCADE
);
"""
)

cursor.execute(
    f""" CREATE TABLE permissions (
	id INTEGER PRIMARY KEY ASC,
    public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL);
"""
)

cursor.execute(
    f""" CREATE TABLE dataset_permissions (
	dataset_id INTEGER,
    permission_id INTEGER,
    PRIMARY KEY(dataset_id, permission_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE);
"""
)

rdfViewId = str(uuid.uuid7())

cursor.execute(
    f"INSERT INTO permissions (id, public_id, name) VALUES (1, '{rdfViewId}', 'rdf:view');"
)


cursor.execute(
    f""" CREATE TABLE samples (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	dataset_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
    reads INTEGER NOT NULL,
	FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
);"""
)

cursor.execute(
    f"""CREATE TABLE bins (
	id INTEGER PRIMARY KEY,
	size INTEGER NOT NULL UNIQUE);
);"""
)

bin_map = {}
for size in bin_sizes:
    bin_map[size] = len(bin_map) + 1
    cursor.execute(f"""INSERT INTO bins (id, size) VALUES (NULL, {size});""")


cursor.execute(
    f"""CREATE TABLE sample_bins (
	id INTEGER PRIMARY KEY,
    sample_id INTEGER NOT NULL,
	size_id INTEGER NOT NULL,
	reads INTEGER NOT NULL,
	bpm_scale_factor REAL NOT NULL,
    FOREIGN KEY(sample_id) REFERENCES samples(id) ON DELETE CASCADE,
    FOREIGN KEY(size_id) REFERENCES bins(id) ON DELETE CASCADE;
);"""
)

cursor.execute(
    f"""CREATE TABLE chromosomes (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);"""
)

cursor.execute(
    f"""CREATE TABLE reads (
    id INTEGER PRIMARY KEY,
    sample_id INTEGER NOT NULL,
	chr_id INTEGER NOT NULL,
	bin_id INTEGER NOT NULL,
	start INTEGER NOT NULL,
	end INTEGER NOT NULL,
	count INTEGER NOT NULL,
	PRIMARY KEY (chr_id, bin, start),
    FOREIGN KEY(sample_id) REFERENCES samples(id),
	FOREIGN KEY(chr_id) REFERENCES chromosomes(id),
	FOREIGN KEY(bin_id) REFERENCES bins(id) ON DELETE CASCADE);
);"""
)

cursor.execute("COMMIT;")


cursor.execute("BEGIN TRANSACTION;")


df_samples = pd.read_csv("samples.txt", sep="\t", header=0)


current_dataset = None
dataset_map = {}

for i, row in df_samples.iterrows():
    dataset = row["dataset"]
    sample = row["sample"]

    if current_dataset == None or dataset != current_dataset:
        dataset_id = uuid.uuid7()
        dataset_map[dataset] = {"uuid": uuid.uuid7(), "index": len(dataset_map) + 1}
        technology_id = technology_map[row["type"]]
        assembly_id = assembly_map[row["assembly"]]

        cursor.execute(
            f"""INSERT INTO datasets (id, public_id, genome_id, assembly_id, technology_id, name) VALUES (
                {dataset_map[dataset]["index"]}, 
                '{dataset_map[dataset]["uuid"]}', 
                1, 
                {assembly_id}, 
                {technology_id}, 
                '{dataset}');
            """,
        )

    writer = libseq.BinCountWriter(
        datasetId,
        sample,
        bam,
        genome,
        assembly,
        bin_sizes=bin_sizes,
        platform=platform,
        outdir=outdir,
    )
    writer.write_sample_sql(paired=paired)

# writer = libseq.BinCountWriter("CB4_BCL6_RK040_hg19.sorted.rmdup.bam", "hg19", bin_width=1000)
# writer.write_all_chr_sql()
