genome=Human
assembly=grch38
dir=/home/antony/development/data/modules/seqs/${assembly}/RNA-seq
bin_widths=50,100,1000,10000 # 16,64,256,1024,4096,16384 #64,128,256,512,1024

cat rnaseq.txt | sed 1d | grep -v '#' | while read line
do
    sample=`echo "${line}" | cut -f1`
    bam=`echo "${line}" | cut -f2`
  
    echo ${sample}
    echo ${bam}

    outdir=${dir}/RDF_Lab #/${sample}
    python bamtosql.py --sample=${sample} --bam=${bam} --assembly=${assembly} --genome=${genome} --widths=${bin_widths} --out=${outdir} --platform="RNA-seq"
    ./step2_create_db.sh ${sample} ${outdir}
    #break
done
