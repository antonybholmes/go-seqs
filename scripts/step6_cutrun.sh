genome=grch38
dir=/home/antony/development/data/modules/seqs/${genome}/Cut_And_Run
bin_widths=10,100,1000,10000 #64,128,256,512,1024

cat cutrun.txt | sed 1d | grep -v '#' | while read line
do
    sample=`echo "${line}" | cut -f1`
    bam=`echo "${line}" | cut -f2`
  
    echo ${sample}
    echo ${bam}

    outdir=${dir}/RDF_Lab/${sample}
    python bamtosql.py --sample=${sample} --bam=${bam} --genome=${genome} --widths=${bin_widths} --out=${outdir} --platform="Cut_And_Run" --paired
    ./step2_create_db.sh ${sample} ${outdir}
    #break
done
