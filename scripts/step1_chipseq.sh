genome=Human
assembly=hg19
dir=/home/antony/development/data/modules/seqs/${assembly}/ChIP-seq
bin_widths=50,100,1000,10000 # 16,64,256,1024,4096,16384 # 10,100,1000,10000 #50,500,5000 #64,128,256,512,1024

cat chipseq.txt | sed 1d | grep -v '#'| while read line
do
    bam=`echo "${line}" | cut -f1`
    dataset=`echo "${line}" | cut -f2`
    #bam=/ifs/scratch/cancer/Lab_RDF/ngs/chip_seq/data/human/rdf/hg19/rdf/katia/data/CB4_BCL6_RK040/reads/CB4_BCL6_RK040_hg19.sorted.rmdup.bam
    sample=`basename ${bam} | sed -r 's/\..+//'`
    
    echo ${sample}
    echo ${bam}

    outdir=${dir}/${dataset} #/${sample}
    cat sample.sql > ${outdir}/${sample}.sql
    echo >> ${outdir}/${sample}.sql
    echo >> ${outdir}/${sample}.sql
    python bamtosql.py --sample=${sample} --bam=${bam} --assembly=${assembly} --genome=${genome} --widths=${bin_widths} --out=${outdir}
    
    #rm ${outdir}/${sample}.db #sample.db
    #cat sample.sql | sqlite3 ${outdir}/${sample}.db
    #cat ${outdir}/${sample}_header.sql | sqlite3 ${outdir}/${sample}.db
    #cat ${outdir}/${sample}.sql | sqlite3 ${outdir}/${sample}.db

    ./step2_create_db.sh ${sample} ${outdir}

    #break
done
