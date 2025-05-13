# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3
from nanoid import generate

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dir", help="sample name")
args = parser.parse_args()

dir = args.dir  # sys.argv[1]

data = []

for root, dirs, files in os.walk(dir):
    for filename in files:
        if filename == "track.db":
            relative_dir = root.replace(dir, "")[1:]

            print(relative_dir)

            genome, platform, dataset, sample = relative_dir.split("/")

            dataset = dataset.replace("_", " ")

            # filepath = os.path.join(root, filename)
            print(root, filename, relative_dir, platform, genome, dataset, sample)

            conn = sqlite3.connect(os.path.join(root, filename))

            # Create a cursor object
            cursor = conn.cursor()

            # Execute a query to fetch data
            cursor.execute("SELECT public_id, genome, platform, name, reads FROM track")

            # Fetch all results
            results = cursor.fetchall()

            # Print the results
            for row in results:
                row = list(row)
                # row.append(generate("0123456789abcdefghijklmnopqrstuvwxyz", 12))
                row.append(dataset)
                row.append("Seq")
                row.append(relative_dir)
                row.append(dataset)
                data.append(row)

            conn.close()

with open(os.path.join(dir, "tracks.sql"), "w") as f:
    print("BEGIN TRANSACTION;", file=f)
    for row in data:
        values = ", ".join([f"'{v}'" for v in row])
        print(
            f"INSERT INTO tracks (public_id, genome, platform, name, reads, dataset, track_type, url, tags) VALUES ({values});",
            file=f,
        )

    print("COMMIT;", file=f)
