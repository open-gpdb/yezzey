
export GPHOME=/usr/local/gpdb/
source /usr/local/gpdb/greenplum_path.sh

make clean && make -j32 install
make destroy-demo-cluster && make create-demo-cluster
source gpAux/gpdemo/gpdemo-env.sh

gpconfig -c yezzey.use_gpg_crypto -v "false"
gpconfig -c shared_preload_libraries -v yezzey

gpstop -a -i && gpstart -a


createdb $USER
