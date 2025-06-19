load_number=0
run_number=200000000
load_data_path='/d/dataset/YCSB/ycsb200M/ycsb_load_workloada'
run_data_path='/d/dataset/YCSB/ycsb200M/ycsb_run_workloada'
pm_path='/d/dataset/YCSB/ycsb200M/ycsb_run_workloada'
thread_number=32
operations='insert'

mkdir build
cd build
cmake ..
make clean
make

./PIONEER --load_number=$load_number --run_number=$run_number --load_data_path=$load_data_path --run_data_path=$run_data_path --pm_path=$pm_path --thread_number=$thread_number --operations=$operations