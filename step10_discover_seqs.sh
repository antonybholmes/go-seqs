dir=data/modules/seqs

python discover_seqs.py --dir=${dir} 

 
rm ${dir}/tracks.db
cat tracks.sql | sqlite3 ${dir}/tracks.db
cat ${dir}/tracks.sql | sqlite3 ${dir}/tracks.db
