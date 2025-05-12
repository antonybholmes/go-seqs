genome=hg19
dir=/home/antony/development/data/modules/seqs/${genome}/ChIP-seq
 
rm ${dir}/bigwig.sql

python trackdbtosql.py --file="/ifs/scratch/cancer/Lab_RDF/ngs/chip_seq/data/human/rdf/hg19/rdf/elodie_dlbcl_cell_lines_29/analysis/hub_elodie_cell_lines_bpm/hg19/trackDb.txt" \
    --dataset="RDF_29CL" \
    --genome=${genome} \
    --out="${dir}/bigwig.sql"
