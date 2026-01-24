dir=../data/modules/seqs

python discover_seqs.py --dir=${dir} 
cat /home/antony/development/data/modules/seqs/hg19/ChIP-seq/bigwig.sql >> ${dir}/samples.sql
 
rm ${dir}/samples.db
cat samples.sql | sqlite3 ${dir}/samples.db
cat ${dir}/samples.sql | sqlite3 ${dir}/samples.db
