# for f in `ls *.sql | grep -v table`
# do
#     name=`echo ${f} | sed -r 's/.sql//'`
#     rm ${name}.db
#     cat tables.sql | sqlite3 ${name}.db
#     cat ${f} | sqlite3 ${name}.db
# done

sample=$1
dir=$2

for f in `ls ${dir}/chr*sql | sort`
do
	echo ${f}
	db=`echo ${f} | sed -r 's/sql/db/'`
	rm ${db}
	cat tables.sql | sqlite3 ${db}
	cat ${f} | sqlite3 ${db}
done
