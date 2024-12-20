genome=hg19
dir=/home/antony/development/data/modules/tracks/ChIP-seq
bin_width=100,1000

sample=CB4_BCL6_RK040_hg19
outdir=${dir}/${genome}/${sample}
bam=/ifs/scratch/cancer/Lab_RDF/ngs/chip_seq/data/human/rdf/hg19/rdf/katia/data/CB4_BCL6_RK040/reads/CB4_BCL6_RK040_hg19.sorted.rmdup.bam
python bamtosql.py --sample=${sample} --bam=${bam} --genome=${genome} --widths=${bin_width} --out=${outdir}
./step2_create_db.sh ${sample} ${outdir}

exit(0)

sample=CB4_H3K27Ac_RK043_hg19
outdir=${dir}/${genome}/${sample}
bam=/ifs/scratch/cancer/Lab_RDF/ngs/chip_seq/data/human/rdf/hg19/rdf/katia/data/CB4_H3K27Ac_RK043/reads/CB4_H3K27Ac_RK043_hg19.sorted.rmdup.bam
python bamtosql.py --sample=${sample} --bam=${bam} --genome=${genome} --widths=${bin_width} --out=${outdir}
./step2_create_db.sh ${sample} ${outdir}

exit(0)

