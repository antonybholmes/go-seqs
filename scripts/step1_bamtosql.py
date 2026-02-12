# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import collections
import math
import os
import re
import sqlite3
import sys

import libbam
import libseq
import numpy as np
import pandas as pd
import uuid_utils as uuid
from nanoid import generate

DIR = "../data/modules/seqs"
rdfViewId = str(uuid.uuid7())

parser = argparse.ArgumentParser()

parser.add_argument("-w", "--widths", default="50,100,1000,10000", help="size of bin")
parser.add_argument("-o", "--out", default=DIR, help="output directory")
parser.add_argument(
    "--samples",
    default="samples.tsv",
    help="tsv file with columns: dataset, sample, paired, bam, genome, assembly, type",
)
parser.add_argument(
    "--no-create-samples", action="store_true", help="data is paired end"
)
parser.add_argument(
    "--mode",
    default="default",
    help="mode for reducing bin variation to make smaller bins. round2 rounds to nearest multiple of 2",
)

parser.add_argument(
    "--min-reads",
    default="4",
    help="mode for reducing bin variation to make smaller bins. round2 rounds to nearest multiple of 2",
)

args = parser.parse_args()


bin_sizes = [int(w) for w in args.widths.split(",")]
outdir = args.out
samples_file = args.samples
create_samples = not args.no_create_samples
mode = args.mode
min_reads = int(args.min_reads)

print("mode", mode, create_samples, min_reads)


df_samples = pd.read_csv(samples_file, sep="\t", header=0, keep_default_na=False)

df_seq_samples = df_samples[df_samples["type"] == "Seq"]
df_remote_bigwig_samples = df_samples[df_samples["type"] == "Remote BigWig"]

# df_samples = df_samples.iloc[0:1, :] # testing with just 1 sample for now

genome_map = {"Human": 1, "Mouse": 2}
assembly_map = {"hg19": 1, "GRCh38": 2, "GRCm39": 3}
technology_map = {"ChIP-seq": 1, "RNA-seq": 2, "CUT&RUN": 3}
type_map = {"Seq": 1, "Remote BigWig": 2}

current_dataset = None
dataset_map = {}

for i, row in df_seq_samples.iterrows():
    dataset = row["dataset"]
    sample = row["sample"]
    paired = row["paired"] == "True"
    bam = row["file"]
    genome = row["genome"]
    assembly = row["assembly"]
    technology = row["technology"]

    if not create_samples:
        continue

    out = re.sub(r" +", "_", sample)

    dir = os.path.join(outdir, assembly, technology, dataset)

    dir = re.sub(r" +", "_", dir).replace("&", "_AND_")

    os.makedirs(dir, exist_ok=True)

    db = os.path.join(
        dir,
        f"{re.sub(r' +', '_', sample)}.db",
    )

    print("sample db", db)

    if os.path.exists(db):
        os.remove(db)

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA foreign_keys = ON;")

    cursor.execute(
        f""" CREATE TABLE sample (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        dataset TEXT NOT NULL,
        genome TEXT NOT NULL,
        assembly TEXT NOT NULL,
        technology TEXT NOT NULL,
        name TEXT NOT NULL UNIQUE,
        type TEXT NOT NULL DEFAULT 'Seq',
        reads INTEGER NOT NULL DEFAULT 0,
        url TEXT NOT NULL DEFAULT '');
    """
    )

    cursor.execute(
        f"""CREATE TABLE bins (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        size INTEGER NOT NULL UNIQUE,
        reads INTEGER NOT NULL DEFAULT 0,
        bpm_scale_factor REAL NOT NULL DEFAULT 1.0);
    """
    )

    bin_map = {}
    for bi, size in enumerate(bin_sizes):
        bin_map[size] = len(bin_map) + 1
        cursor.execute(
            f"""INSERT INTO bins (id, public_id, size) VALUES ({bi+1}, '{str(uuid.uuid7())}', {size});"""
        )

    cursor.execute(
        f"""CREATE TABLE chromosomes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE);
        """
    )

    cursor.execute(
        f"""CREATE TABLE reads (
        id INTEGER PRIMARY KEY,
        chr_id INTEGER NOT NULL,
        bin_id INTEGER NOT NULL,
        start INTEGER NOT NULL,
        end INTEGER NOT NULL,
        count INTEGER NOT NULL,
        UNIQUE(chr_id, bin_id, start),
        FOREIGN KEY (chr_id) REFERENCES chromosomes(id),
        FOREIGN KEY (bin_id) REFERENCES bins(id) ON DELETE CASCADE);
    """
    )

    reader = libbam.BamReader(bam, paired=paired)

    chrs = reader.chrs()

    chr = ""
    c = 0

    total_reads = 0
    bin_reads_map = collections.defaultdict(int)

    # sometimes bam_chr does not contain chr prefix
    for chri, bam_chr in enumerate(chrs):
        if "_" in bam_chr:
            # only encode official chr
            continue

        # testing with just chr1 for now
        # if chri == 1:
        #    break

        chr = bam_chr
        if not chr.startswith("chr"):
            chr = "chr" + chr

        cursor.execute(
            f"INSERT INTO chromosomes (id, public_id, name) VALUES ({chri+1}, '{str(uuid.uuid7())}', '{chr}');"
        )

        chr_read_map = collections.defaultdict(int)

        bin_map = collections.defaultdict(lambda: collections.defaultdict(int))

        # self._reset()
        c = 0
        print("Processing sql", chr, "...")
        chr_reads = 0

        for read in reader.reads(bam_chr):
            if paired:
                # in the pair, pick the min start
                start = min(read.pos, read.pnext) - 1
                read_length = abs(read.tlen)
                # print("paired", start, read_length)
            else:
                start = read.pos - 1
                read_length = read.length
                # print(start, read_length)

            # for all bins calc unique reads per bin
            for bin_width in bin_sizes:
                sb = math.floor(start / bin_width)
                eb = math.floor((start + read_length - 1) / bin_width)

                for b in range(sb, eb + 1):
                    bin_map[bin_width][b] += 1
                    # for each bin size, count total reads spanning
                    # the bins
                    bin_reads_map[bin_width] += 1

            if c % 100000 == 0:
                print("Processed", str(c), "reads...")

            chr_reads += 1
            c += 1

        chr_read_map[chr] = chr_reads
        total_reads += chr_reads

        print("chr reads", chr, chr_reads)

        for bin_sizei, bin_size in enumerate(bin_sizes):

            # set small counts to zero to reduce what is basically noise

            bins = sorted(bin_map[bin_size])

            smooth_bin_map = collections.defaultdict(int)

            # test all bins between ends for smoothing
            for bi in range(bins[0], bins[-1] + 1):  # bins:
                c = bin_map[bin_size][bi] if bi in bin_map[bin_size] else 0

                if mode == "round2":
                    # round to nearest multiple of 2 so that we reduce
                    # bin variation to make smaller bins
                    # ca = np.ceil(ca * 0.5) * 2
                    c = np.ceil(c * 0.5) * 2

                if c > min_reads:
                    smooth_bin_map[bi] = c  # {"reads": c, "max_height": max_height}

            bins = sorted(smooth_bin_map)

            if len(bins) == 0:
                # nothing to write for this bin size
                continue

            max_bin = bins[-1]

            # merge contiguous blocks with same count
            res = []
            current_count = smooth_bin_map[bins[0]]
            start_bin = bins[0]

            for bi in range(bins[0], bins[-1] + 1):
                reads = smooth_bin_map[bi] if bi in smooth_bin_map else 0

                if reads != current_count:
                    if current_count > 0:
                        start1 = start_bin * bin_size + 1
                        end1 = bi * bin_size  # + 1
                        kb = (end1 - start1 + 1) / 1000
                        res.append(
                            {
                                "start": start1,
                                "end": end1,
                                "reads": current_count,
                                "rpk": current_count / kb,
                                "bpm": 0,
                            }
                        )

                    current_count = reads
                    start_bin = bi

            if current_count > 0:
                # in this 1 based system, start and end are inclusive
                start1 = start_bin * bin_size + 1
                end1 = (bins[-1] + 1) * bin_size
                kb = (end1 - start1 + 1) / 1000
                res.append(
                    {
                        "start": start1,
                        "end": end1,
                        "reads": current_count,
                        "rpk": current_count / kb,
                        "bpm": 0,
                    }
                )

            for b in res:
                cursor.execute(
                    f"""INSERT INTO reads (chr_id, bin_id, start, end, count) VALUES (
                        {chri+1}, 
                        {bin_sizei+1}, 
                        {b['start']}, 
                        {b['end']}, 
                        {b['reads']});
                    """
                )

    cursor.execute(
        f"""INSERT INTO sample (id, public_id, dataset, genome, assembly, technology, name, reads) VALUES (
            1, 
            '{str(uuid.uuid7())}', 
            '{dataset}', 
            '{genome}', 
            '{assembly}', 
            '{technology}', 
            '{sample}', 
            {total_reads});
        """
    )

    for bin in bin_sizes:
        #  BPM (per bin) = number of reads per bin / sum of all reads per bin (in millions) so
        # BPM = 1000000 * (number of reads per bin) / sum of all reads per bin) where
        # sum of all reads per bin = total reads spanning bins and can be greater than total reads in library
        bin_reads = bin_reads_map[bin]
        bpm_scale_factor = 1000000 / bin_reads if bin_reads > 0 else 0

        print(
            f"INSERT INTO bins (public_id, size, reads, bpm_scale_factor) VALUES ('{str(uuid.uuid7())}', {bin}, {bin_reads}, {bpm_scale_factor});"
        )

        cursor.execute(
            f"UPDATE bins SET reads = {bin_reads}, bpm_scale_factor = {bpm_scale_factor} WHERE size = {bin};"
        )

    cursor.close()
    conn.commit()
    conn.close()


# writer = libseq.BinCountWriter("CB4_BCL6_RK040_hg19.sorted.rmdup.bam", "hg19", bin_width=1000)
# writer.write_all_chr_sql()


db = os.path.join(outdir, "seqs.db")


print("dataset map", db)

if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")


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

cursor.execute(
    f"""
    CREATE TABLE assemblies (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        genome_id INTEGER NOT NULL,
        name TEXT NOT NULL UNIQUE,
        FOREIGN KEY (genome_id) REFERENCES genomes(id) ON DELETE CASCADE);
    """,
)

cursor.execute(
    f"INSERT INTO assemblies (id, public_id, genome_id, name) VALUES (1, '{uuid.uuid7()}', 1, 'hg19');"
)
cursor.execute(
    f"INSERT INTO assemblies (id, public_id, genome_id, name) VALUES (2, '{uuid.uuid7()}', 1, 'GRCh38');"
)
cursor.execute(
    f"INSERT INTO assemblies (id, public_id, genome_id, name) VALUES (3, '{uuid.uuid7()}', 2, 'GRCm39');"
)

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


cursor.execute(
    f""" CREATE TABLE datasets (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	assembly_id INTEGER NOT NULL,
    name TEXT NOT NULL, 
    description TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(assembly_id) REFERENCES assemblies(id) ON DELETE CASCADE
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
    f"""CREATE TABLE dataset_permissions (
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
    f""" CREATE TABLE sample_types (
	id INTEGER PRIMARY KEY ASC,
    public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL);
"""
)

cursor.execute(
    f"INSERT INTO sample_types (id, public_id, name) VALUES (1, '{uuid.uuid7()}', 'Seq');"
)
cursor.execute(
    f"INSERT INTO sample_types (id, public_id, name) VALUES (2, '{uuid.uuid7()}', 'Remote BigWig');"
)

cursor.execute(
    f""" CREATE TABLE samples (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	dataset_id INTEGER NOT NULL,
    technology_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
    type_id INTEGER NOT NULL,
    reads INTEGER NOT NULL DEFAULT 0,
    url TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY(technology_id) REFERENCES technologies(id) ON DELETE CASCADE,
    FOREIGN KEY(type_id) REFERENCES sample_types(id) ON DELETE CASCADE
);"""
)


for root, dirs, files in os.walk(outdir):
    if "trash" in root:
        continue

    for filename in files:
        if filename == "seqs.db":
            continue

        if filename == "samples.db":
            continue

        if filename == "tracks.db":
            continue

        if not filename.endswith(".db"):
            continue

        relative_dir = root.replace(outdir, "")[1:]

        assembly, technology, dataset_name = relative_dir.split("/")

        technology = technology.replace("_AND_", "&")

        sample = filename.replace(".db", "")

        if dataset_name not in dataset_map:
            dataset_id = uuid.uuid7()
            dataset = {
                "public_id": str(uuid.uuid7()),
                "index": len(dataset_map) + 1,
                "assembly": assembly_map[assembly],
                "name": dataset_name,
            }

            dataset_map[dataset_name] = dataset

            print(dataset)

            cursor.execute(
                f"""INSERT INTO datasets (id, public_id, assembly_id, name) VALUES (
                    {dataset["index"]},
                    '{dataset["public_id"]}',
                    {dataset["assembly"]},
                    '{dataset["name"]}');""",
            )

        dataset = dataset_map[dataset_name]

        # filepath = os.path.join(root, filename)
        print(root, filename, relative_dir, technology, assembly, dataset, sample)

        conn2 = sqlite3.connect(os.path.join(root, filename))
        conn2.row_factory = sqlite3.Row

        # Create a cursor object
        cursor2 = conn2.cursor()

        # Execute a query to fetch data
        cursor2.execute(
            "SELECT public_id, genome, assembly, technology, name, reads FROM sample"
        )

        data = []

        # Fetch all results
        results = cursor2.fetchall()

        # Print the results
        for row in results:

            row = {
                "public_id": row["public_id"],
                "assembly": row["assembly"],
                "type_id": type_map["Seq"],
                "technology_id": technology_map[technology],
                "name": row["name"],
                "reads": row["reads"],
                "dataset_id": dataset["index"],
                "url": os.path.join(relative_dir, filename),  # where to find the sql db
            }

            data.append(row)

        conn2.close()

        for row in data:
            cursor.execute(
                f"""INSERT INTO samples (public_id, dataset_id, technology_id, name, type_id, reads, url) VALUES (
                    '{row["public_id"]}',
                    {row["dataset_id"]},
                    {row["technology_id"]},
                    '{row["name"]}',
                    {row["type_id"]},
                    {row["reads"]},
                    '{row["url"]}');
                """,
            )

for i, row in df_remote_bigwig_samples.iterrows():
    # insert the remote bigwig samples as well
    dataset_name = row["dataset"]
    sample = row["sample"]
    genome = row["genome"]
    assembly = row["assembly"]
    technology = row["technology"]
    type = row["type"]
    file = row["file"]
    scale = row["scale"]

    if dataset_name not in dataset_map:
        dataset_id = uuid.uuid7()
        dataset = {
            "public_id": str(uuid.uuid7()),
            "index": len(dataset_map) + 1,
            "assembly": assembly_map[assembly],
            "name": dataset_name,
        }

        dataset_map[dataset_name] = dataset

        print(dataset)

        cursor.execute(
            f"""INSERT INTO datasets (id, public_id, assembly_id, name) VALUES (
                {dataset["index"]},
                '{dataset["public_id"]}',
                {dataset["assembly"]},
                '{dataset["name"]}');""",
        )

    dataset = dataset_map[dataset_name]

    with open(file, "r") as f:
        for line in f:
            line = line.strip()
            tokens = line.split(" ")

            if tokens[0] == "track":
                name = tokens[1]

            if tokens[0] == "bigDataUrl":
                url = tokens[1]

                if "bw" not in url and "bigWig" not in url:
                    print("Warning: url does not seem to be a bigwig", url)
                    continue

                id = str(uuid.uuid7())
                cursor.execute(
                    f"""INSERT INTO samples (public_id, dataset_id, technology_id, name, type_id, reads, url, tags) VALUES (
                    '{id}',
                    {dataset["index"]},
                    {technology_map[technology]},
                    '{name}',
                    {type_map["Remote BigWig"]},
                    -1,
                    '{url}',
                    'scale={scale}');
                """,
                )

cursor.execute(
    f"""INSERT INTO dataset_permissions (dataset_id, permission_id) 
        SELECT id, 1 FROM datasets;""",
)

cursor.execute(f"CREATE INDEX idx_datasets_name_id ON datasets(LOWER(name));")
cursor.execute(f"CREATE INDEX idx_samples_name_id ON samples(LOWER(name));")
conn.commit()
conn.close()
