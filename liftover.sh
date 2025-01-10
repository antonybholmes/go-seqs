 
python liftover.py \
    --d /home/antony/development/go/go-seqs/data/modules/seqs/grch38/Cut_And_Run/ \
    --o /home/antony/development/go/go-seqs/data/modules/seqs
exit(0) 
for dir in `find /home/antony/development/go/data/modules/seqs/hg19/Cut_And_Run/RDF_Lab/ -maxdepth 1 -mindepth 1 -type d`
do
    echo ${dir}
    sample=`basename ${dir}`
    ./step2_create_db.sh ${sample} ${dir}
done

exit(0) 

python liftover.py \
--d /home/antony/development/go/go-seqs/data/modules/seqs/grch38/RNA-seq/ \
--o /home/antony/development/go/go-seqs/data/modules/seqs


exit(0)

for dir in `find /home/antony/development/go/data/modules/seqs/hg19/RNA-seq/RDF_Lab/ -maxdepth 1 -mindepth 1 -type d`
do
    echo ${dir}
    sample=`basename ${dir}`
    ./step2_create_db.sh ${sample} ${dir}
done