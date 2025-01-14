# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3
import subprocess
from nanoid import generate

BINS = [10,100,1000,10000]

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dir", help="input directory")
parser.add_argument("-o", "--outdir", help="output directory")
parser.add_argument("-f", "--from_genome", default="grch38", help="from genome")
parser.add_argument("-t", "--to_genome", default="hg19", help="to genome")
args = parser.parse_args()

dir = args.dir  # sys.argv[1]
outdir = args.outdir  # sys.argv[1]
from_genome = args.from_genome  # sys.argv[1]
to_genome = args.to_genome  # sys.argv[1]

data = []
publicId = ""

for root, dirs, files in os.walk(dir):
    dirs.sort()
    files.sort()
    for filename in files:
        if filename == "track.db":
            print(root)
            genome, platform, dataset, sample = root.split("/")[-4:]

            dataset = dataset.replace("_", " ")

            # filepath = os.path.join(root, filename)
            print(root, filename, root, platform, genome, dataset, sample)

            conn = sqlite3.connect(os.path.join(root, filename))

            # Create a cursor object
            cursor = conn.cursor()

            # Execute a query to fetch data
            cursor.execute("SELECT public_id, genome, platform, name, reads FROM track")

            sample_out_dir = os.path.join(
                outdir,
                to_genome,
                platform,
                dataset.replace(" ", "_"),
                sample + f"_{to_genome}_liftover",
            )

            os.makedirs(sample_out_dir, exist_ok=True)

            print(sample_out_dir)

            # Fetch all results
            result = list(cursor.fetchone())

            publicId = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
            result[0] = publicId
            result[1] = to_genome
            result[3] += f"_liftover"

            values = ", ".join([f"'{v}'" for v in result])
            print(values)
            with open(os.path.join(sample_out_dir, "track.sql"), "w") as f:
                print(
                    f"INSERT INTO track (public_id, genome, platform, name, reads) VALUES ({values});",
                    file=f,
                )

            conn.close()

        if filename.endswith(".db") and filename != "track.db":
            genome, platform, dataset, sample = root.split("/")[-4:]

            # a bin file to convert
            print(filename)

            sample_out_dir = os.path.join(
                outdir,
                to_genome,
                platform,
                dataset,
                sample + f"_{to_genome}_liftover"
            )

            os.makedirs(sample_out_dir, exist_ok=True)

            out = os.path.join(
                sample_out_dir,
                filename.replace(".db", ".sql").replace(from_genome, to_genome),
            )

            with open(out, "w") as fout:
                conn = sqlite3.connect(os.path.join(root, filename))

                # Create a cursor object
                cursor = conn.cursor()

                # Execute a query to fetch data
                cursor.execute(
                    "SELECT public_id, genome, platform, name, chr, reads FROM track"
                )

                result = list(cursor.fetchone())

                # give it a new id
                result[0] = publicId

                result[2] += f"_{to_genome}_liftover"

                chr = result[4]

                values = ", ".join([f"'{v}'" for v in result])

                print(
                    f"INSERT INTO track (public_id, genome, platform, name, chr, reads) VALUES ({values});",
                    file=fout,
                )

                #
                # clone the scale factors
                #
                print("BEGIN TRANSACTION;", file=fout)

                cursor.execute(
                    "SELECT bin_size, scale_factor FROM bpm_scale_factors"
                )

                rows = cursor.fetchall()

                for row in rows:
                    print(
                        f"INSERT INTO bpm_scale_factors (bin_size, scale_factor) VALUES ({row[0]}, {row[1]});",
                        file=fout,
                    )

                print("COMMIT;", file=fout)

                

                for bin in BINS:

                    cursor.execute(f"SELECT start, end, reads FROM bins{bin}")

                    rows = cursor.fetchall()

                    with open("tmp.bed", "w") as f:

                        for row in rows:
                            start = row[0]
                            end = row[1]
                            reads = row[2]

                            print(
                                f"{chr}\t{start}\t{end}\t{reads}",
                                file=f,
                            )

                    # /ifs/scratch/cancer/Lab_RDF/ngs/tools/ucsc/liftOver tmp.bed /ifs/scratch/cancer/Lab_RDF/ngs/references/ucsc/liftover/hg38ToHg19.over.chain tmp1.bed unmapped

                    res = subprocess.run(
                        [
                            "/ifs/scratch/cancer/Lab_RDF/ngs/tools/ucsc/liftOver",
                            "tmp.bed",
                            "/ifs/scratch/cancer/Lab_RDF/ngs/references/ucsc/liftover/hg38ToHg19.over.chain",
                            f"tmp_{to_genome}.bed",
                            "unmapped",
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )

                    if res.returncode == 0:
                        # print("Command succeeded")

                        # use the liftover bed to make a new bins file
                        print("BEGIN TRANSACTION;", file=fout)

                        used = set()

                        with open(f"tmp_{to_genome}.bed", "r") as fin:
                            for line in fin:
                                tokens = line.strip().split("\t")

                                # if starts resolve to same location, keep the first
                                if tokens[1] in used:
                                    continue

                                print(
                                    f"INSERT INTO bins{bin} (start, end, reads) VALUES ({tokens[1]}, {tokens[2]}, {tokens[3]});",
                                    file=fout,
                                )

                                used.add(tokens[1])

                        print("COMMIT;", file=fout)
