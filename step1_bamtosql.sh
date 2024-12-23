genome=hg19
dir=/home/antony/development/data/modules/seqs/ChIP-seq
bin_widths=128,256,512,1024

cat samples.txt | sed 1d | while read bam
do
    #bam=/ifs/scratch/cancer/Lab_RDF/ngs/chip_seq/data/human/rdf/hg19/rdf/katia/data/CB4_BCL6_RK040/reads/CB4_BCL6_RK040_hg19.sorted.rmdup.bam
    sample=`basename ${bam} | sed -r 's/\..+//'`
    
    echo ${sample}
    echo ${bam}

    outdir=${dir}/${genome}/${sample}
    python bamtosql.py --sample=${sample} --bam=${bam} --genome=${genome} --widths=${bin_widths} --out=${outdir}
    ./step2_create_db.sh ${sample} ${outdir}
done
