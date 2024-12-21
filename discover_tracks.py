# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dir", help="sample name")
args = parser.parse_args()

dir = args.dir  # sys.argv[1]

data = []

for root, dirs, files in os.walk(dir):
    for filename in files:
        if filename == 'track.db':
            relative_dir = root.replace(dir, '')[1:]
            platform, genome, sample = relative_dir.split("/")
            #filepath = os.path.join(root, filename)
            print(root, filename, relative_dir, platform, genome, sample)

            conn = sqlite3.connect( os.path.join(root, filename))

            # Create a cursor object
            cursor = conn.cursor()

            # Execute a query to fetch data
            cursor.execute('SELECT platform, genome, public_id, name, reads, stat_mode FROM track')

            # Fetch all results
            results = cursor.fetchall()

            # Print the results
            for row in results:
                row = list(row)
                row.append(relative_dir)
                data.append(row)
               
            conn.close()

with open(os.path.join(dir, "tracks.sql"), "w") as f:
    print("BEGIN TRANSACTION;", file=f)
    for row in data:
         
        values  = ', '.join([f"'{v}'" for v in row])
        print(f"INSERT INTO tracks (platform, genome, public_id, name, reads, stat_mode, dir) VALUES ({values});", file=f)

    print("COMMIT;", file=f)