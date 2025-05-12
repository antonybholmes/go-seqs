dir=data/modules/seqs

python discover_seqs.py --dir=${dir} 
cat /home/antony/development/data/modules/seqs/hg19/ChIP-seq/bigwig.sql >> ${dir}/tracks.sql
 
rm ${dir}/tracks.db
cat tracks.sql | sqlite3 ${dir}/tracks.db
cat ${dir}/tracks.sql | sqlite3 ${dir}/tracks.db
