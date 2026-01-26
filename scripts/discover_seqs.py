# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3

import uuid_utils as uuid

genome_map = {
    "hg19": "Human",
    "hg38": "Human",
    "grch38": "Human",
    "mm10": "Mouse",
    "rn6": "Rat",
}

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dir", help="sample name")
args = parser.parse_args()

dir = args.dir  # sys.argv[1]

data = []

datasets = {}

for root, dirs, files in os.walk(dir):
    for filename in files:
        if filename == "sample.db":
            relative_dir = root.replace(dir, "")[1:]

            print(relative_dir)

            assembly, platform, dataset_name, sample = relative_dir.split("/")

            genome = genome_map.get(assembly.lower(), assembly)

            dataset_name = dataset_name.replace("_", " ")

            if dataset_name not in datasets:
                dataset_id = uuid.uuid7()
                datasets[dataset_name] = {
                    "id": dataset_id,
                    "name": dataset_name,
                    "platform": platform,
                    "genome": genome,
                    "assembly": assembly,
                }

            dataset = datasets[dataset_name]

            # filepath = os.path.join(root, filename)
            print(root, filename, relative_dir, platform, genome, dataset, sample)

            conn = sqlite3.connect(os.path.join(root, filename))
            conn.row_factory = sqlite3.Row

            # Create a cursor object
            cursor = conn.cursor()

            # Execute a query to fetch data
            cursor.execute(
                "SELECT id, genome, assembly, platform, name, reads FROM sample"
            )

            # Fetch all results
            results = cursor.fetchall()

            # Print the results
            for row in results:
                row = {
                    "id": row["id"],
                    "genome": row["genome"],
                    "assembly": row["assembly"],
                    "platform": row["platform"],
                    "name": row["name"],
                    "reads": row["reads"],
                    "dataset_id": dataset["id"],
                    "type": "Seq",
                    "url": relative_dir,
                }
                # row.append(generate("0123456789abcdefghijklmnopqrstuvwxyz", 12))
                # row.append(dataset["id"])
                # row.append("Seq")
                # row.append(relative_dir)
                # row.append(dataset)
                data.append(row)

            conn.close()

with open(os.path.join(dir, "samples.sql"), "w") as f:

    print("BEGIN TRANSACTION;", file=f)
    for [dataset_name, dataset] in datasets.items():

        print(
            f"""INSERT INTO datasets (id, genome, assembly, platform, name) VALUES (
                '{dataset["id"]}',
                '{dataset["genome"]}',
                '{dataset["assembly"]}',
                '{dataset["platform"]}',
                '{dataset["name"]}');""",
            file=f,
        )

        print(
            f"INSERT INTO dataset_permissions (dataset_id, permission_id) VALUES ('{dataset["id"]}', '019bebfc-30dc-7569-8727-02c741227ad8');",
            file=f,
        )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)
    for row in data:

        print(
            f"""INSERT INTO samples (id, dataset, genome, assembly, platform, name, reads, type, url) VALUES (
                '{row["id"]}',
                '{row["dataset"]}',
                '{row["genome"]}',
                '{row["assembly"]}',
                '{row["platform"]}',
                '{row["name"]}',
                {row["reads"]},
                '{row["type"]}',
                '{row["url"]}');""",
            file=f,
        )

    print("COMMIT;", file=f)
