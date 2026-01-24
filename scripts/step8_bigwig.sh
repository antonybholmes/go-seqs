genome=Human
assembly=hg19
dir=/home/antony/development/data/modules/seqs/${assembly}/ChIP-seq
 
rm ${dir}/bigwig.sql

python trackdbtosql.py --file="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/chip_seq/data/human/rdf/hg19/rdf/elodie_dlbcl_cell_lines_29/analysis/hub_elodie_cell_lines_bpm/hg19/trackDb.txt" \
    --dataset="RDF_29CL" \
    --genome=${genome} \
    --assembly=${assembly} \
    --out="${dir}/bigwig.sql"
