# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import sys

from nanoid import generate
import libseq

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help="trackdb file")
parser.add_argument("-d", "--dataset", help="dataset")
parser.add_argument("-p", "--platform", default="ChIP-seq", help="platform")
parser.add_argument(
    "-g", "--genome", default="hg19", help="genome sample was aligned to"
)
parser.add_argument("-s", "--scale", default="BPM", help="how bigwig data was scaled")
parser.add_argument("-o", "--out", help="output directory")
args = parser.parse_args()

file = args.file
dataset = args.dataset  # sys.argv[1]
genome = args.genome  # sys.argv[3]
platform = args.platform
scale = args.scale

out = args.out

print(out)

with open(out, "w") as fout:
    with open(file, "r") as f:
        for line in f:
            line = line.strip()
            tokens = line.split(" ")

            if tokens[0] == "track":
                name = tokens[1]

            if tokens[0] == "bigDataUrl":
                url = tokens[1]
                publicId = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
                print(
                    f"INSERT INTO tracks (public_id, genome, platform, name, reads, dataset, track_type, url, tags) VALUES ('{publicId}', '{genome}', '{platform}', '{name}', 0, '{dataset}', 'Remote BigWig', '{url}', 'scale={scale}');",
                    file=fout,
                )
