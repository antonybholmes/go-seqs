dir=/home/antony/development/data/modules/tracks

python discover_tracks.py --dir=${dir} 

 
rm ${dir}/tracks.db
cat tracks.sql | sqlite3 ${dir}/tracks.db
cat ${dir}/tracks.sql | sqlite3 ${dir}/tracks.db
