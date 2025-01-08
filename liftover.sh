genome=hg19
dir=/home/antony/development/data/modules/seqs/${genome}/ChIP-seq
bin_widths=64,128,256,512,1024


python liftover.py \
--d /home/antony/development/go/go-seqs/data/modules/seqs/grch38/RNA-seq/ \
--o /home/antony/development/go/go-seqs/data/modules/seqs


for dir in `find /home/antony/development/go/data/modules/seqs/hg19/RNA-seq/RDF_Lab/ -maxdepth 1 -mindepth 1 -type d`
do
    echo ${dir}
    sample=`basename ${dir}`
    ./step2_create_db.sh ${sample} ${dir}
done