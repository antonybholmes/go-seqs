# for f in `ls *.sql | grep -v table`
# do
#     name=`echo ${f} | sed -r 's/.sql//'`
#     rm ${name}.db
#     cat tables.sql | sqlite3 ${name}.db
#     cat ${f} | sqlite3 ${name}.db
# done

sample=$1
dir=$2

rm ${dir}/sample.db
cat sample.sql | sqlite3 ${dir}/sample.db
cat ${dir}/sample.sql | sqlite3 ${dir}/sample.db

# for f in `find ${dir} | grep -P 'track_bin.+sql$' | sort`
# do
# 	echo ${f}
# 	db=`echo ${f} | sed -r 's/sql/db/'`
# 	echo ${db}
# 	rm ${db}
# 	cat bin_group.sql | sqlite3 ${db}
# 	cat ${f} | sqlite3 ${db}
# done

#for f in `find ${dir} | grep -P 'chr.+sql$' | sort`
for f in `find ${dir} | grep -P 'chr.+sql$' | grep -v 'track_bin' | sort`
do
	echo ${f}
	db=`echo ${f} | sed -r 's/sql/db/'`
	echo ${db}
	rm ${db}
	cat bins.sql | sqlite3 ${db}
	cat ${f} | sqlite3 ${db}
done
