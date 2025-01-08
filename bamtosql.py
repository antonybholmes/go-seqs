# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import sys
import libseq

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--sample", help="sample name")
parser.add_argument("-p", "--platform", default="ChIP-seq", help="platform")
parser.add_argument("-b", "--bam", help="bam file")
parser.add_argument(
    "-g", "--genome", default="hg19", help="genome sample was aligned to"
)
parser.add_argument("-w", "--widths", default="100,1000", help="size of bin")
parser.add_argument("-o", "--out", help="output directory")
args = parser.parse_args()

sample = args.sample  # sys.argv[1]
bam = args.bam  # sys.argv[2]
genome = args.genome  # sys.argv[3]
platform = args.platform
bin_widths = [int(w) for w in args.widths.split(",")]
outdir = args.out

# lib.encode.encode_sam_16bit(chr_size_file, file, chr, read_length, window)

print(sample, genome, bin_widths)
writer = libseq.BinCountWriter(sample, bam, genome, bin_widths=bin_widths, platform=platform, outdir=outdir)
writer.write_all_chr_sql()

# writer = libseq.BinCountWriter("CB4_BCL6_RK040_hg19.sorted.rmdup.bam", "hg19", bin_width=1000)
# writer.write_all_chr_sql()
