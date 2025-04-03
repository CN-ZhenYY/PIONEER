#include "PIONEER.h"
#include <cstdlib>
#include <unistd.h>
#include <boost/thread.hpp>
#include "DataGeneration.h"
#include <filesystem>
#include <numeric>
#include <papi.h>
#include <gflags/gflags.h>
#include <immintrin.h>
#include <stdint.h>

#define GENERATE_MSB 24
#define SKEWNESS 0.01

DEFINE_uint64(load_number, 0, "load dataset number");
DEFINE_uint64(run_number, 200000000, "run dataSet number");
DEFINE_string(load_data_path, "/d/dataset/YCSB/ycsb200M/ycsb_load_workloada", "load dataset path");
DEFINE_string(run_data_path, "/d/dataset/YCSB/ycsb200M/ycsb_run_workloada", "run dataset path");
DEFINE_string(pm_path, "/d/dataset/YCSB/ycsb200M/ycsb_run_workloada", "Persist memory path");
DEFINE_uint32(thread_number, 32, "the thread number running in the concurrent environment");
DEFINE_string(operations, "insert", "op: insert/search/update/delete/YCSB");

//char output[FLAGS_run_number/FLAGS_run_number][MAX_LENGTH + 1];
uint8_t  *ops;
uint64_t *keys;
uint64_t *value_lens;
KeyPointer* keyPointer;
ValuePointer* valuePointer;

bool  * statu_log;

void keyProcessForHier(uint64_t * dataSet, int pid) {
#ifdef SIMD
    uint64_t start = (FLAGS_load_number + FLAGS_run_number) / FLAGS_thread_number * pid;
    uint64_t end   = (FLAGS_load_number + FLAGS_run_number) / FLAGS_thread_number * (pid + 1);
    const int SIMD_WIDTH = 8;
    __m512i midTmp = _mm512_set1_epi64(0x00ff);
    __m512i vKFNVPrime64 = _mm512_set1_epi64(1099511628211ULL);
    __m512i mask = _mm512_set1_epi64((1 << 8) - 1);

    for (uint64_t i = start; i < end; i += SIMD_WIDTH) {
        __m512i val = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(&dataSet[i]));
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(&keyPointer[i].key), val);

        __m512i hash = _mm512_set1_epi64(123);
        __m512i tmp_val = val;

        for (int j = 0; j < 8; ++j) {
            __m512i octet = _mm512_and_si512(tmp_val, midTmp);
            tmp_val = _mm512_srli_epi64(tmp_val, 8);
            hash = _mm512_xor_si512(hash, octet);
            hash = _mm512_mullo_epi64(hash, vKFNVPrime64);
        }

        hash = _mm512_slli_epi64(hash, 1);
        hash = _mm512_and_si512(hash, mask);
        hash = _mm512_add_epi64(hash, _mm512_set1_epi64(1 + (i << 16)));

        _mm512_storeu_si512(reinterpret_cast<__m512i*>(&valuePointer[i].value), hash);
    }
#else
    for(uint64_t i = (FLAGS_load_number + FLAGS_run_number) / FLAGS_thread_number * pid; i < (FLAGS_load_number + FLAGS_run_number) / FLAGS_thread_number * (pid + 1); i++){
        keyPointer[i].key = dataSet[i];
        uint64_t val = dataSet[i];
        uint8_t hash = 123;
        for (int j = 0; j < sizeof(uint64_t); j++)
        {
            uint64_t octet = val & 0x00ff;
            val = val >> 8;

            hash = hash ^ octet;
            hash = hash * kFNVPrime64;
        }
        hash = (hash << 1) + 1;
        valuePointer[i].value = hash + (i << 16);
//        valuePointer[i].value = Util::hashfunc(dataSet[i]) + (i << 16);
    }
#endif
}

void keyProcessForHierMulti(uint64_t * dataSet){
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(FLAGS_thread_number);
    for(int i = 0;i < FLAGS_thread_number;i++){
        insertThreads.create_thread(boost::bind(&keyProcessForHier,dataSet,i));
    }
    insertThreads.join_all();
}

void process(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, int *insertNumber,double * insertTime, bool type){
#ifdef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET((pid + 2), &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif

    barrier.wait();
    nsTimer ts;
    ts.start();
    int i = pid;
    int counter = 0;
    for(;i < FLAGS_run_number;i += FLAGS_thread_number){
        if(dramManager->remove(keyPointer[i],valuePointer[i])){
            counter++;
        }
    }
    ts.end();
    insertNumber[pid] = (int)counter;
    insertTime[pid] = (double)ts.duration() / 1000000 / 1000;
}


void processYCSB(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, int *insertNumber,double * insertTime, bool type){
#ifdef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET((pid + 2), &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif

    barrier.wait();
    nsTimer ts;
    ts.start();
    int i = pid;
    int counter = 0;
    for(;i < FLAGS_run_number;i += FLAGS_thread_number){
        if(dramManager->processRequest(keyPointer[i],valuePointer[i],type)){
            counter++;
        }
    }
    ts.end();
    insertNumber[pid] = (int)counter;
    insertTime[pid] = (double)ts.duration() / 1000000 / 1000;
}

void remainProcess(DRAMManagerNUMA* dramManager,int pid,double * insertTime){
    if(FLAGS_thread_number == 1) {
        nsTimer ts;
        ts.start();
        dramManager->processRemainRequestSingleThread(pid);
        ts.end();
        insertTime[pid] += (double)ts.duration() / 1000000 / 1000;
        printf("%d InsertAll time %f\n",pid,(double)ts.duration() / 1000000 / 1000);
    }else {
#ifdef Binding_threads
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(pid + 2, &mask);
        if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
            printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
        }
#endif
        nsTimer ts;
        ts.start();
        dramManager->processRemainRequest(pid, FLAGS_thread_number);
        ts.end();
        insertTime[pid] += (double)ts.duration() / 1000000 / 1000;
    }
}
void removeRemainProcess(DRAMManagerNUMA* dramManager,int pid,double * insertTime){
    if(FLAGS_thread_number == 1) {
        nsTimer ts;
        ts.start();
        dramManager->removeRemainRequestSingleThread(pid);
        ts.end();
        insertTime[pid] += (double)ts.duration() / 1000000 / 1000;
        printf("%d InsertAll time %f\n",pid,(double)ts.duration() / 1000000 / 1000);
    }else {
#ifdef Binding_threads
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(pid + 2, &mask);
        if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
            printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
        }
#endif
        nsTimer ts;
        ts.start();
        dramManager->removeRemainRequest(pid, FLAGS_thread_number);
        ts.end();
        insertTime[pid] += (double)ts.duration() / 1000000 / 1000;
    }
}

void YCSBProcess(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, int *insertNumber,double * insertTime){
#ifdef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(pid + 2, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    usleep(1000);
    barrier.wait();
    nsTimer ts;
    ts.start();
    uint64_t i = pid + FLAGS_load_number;
    uint64_t counter = 0;

    for(;i < FLAGS_run_number + FLAGS_load_number;i += FLAGS_thread_number){
        counter += dramManager->processRequest(keyPointer[i],valuePointer[i],ops[i - FLAGS_load_number]);
    }
    ts.end();
    insertNumber[pid] = (int)counter;
    insertTime[pid] = (double)ts.duration() / 1000000 / 1000;
}

void readDataSet(int pid){
    int offset = FLAGS_load_number/INIT_THREAD_NUMBER * pid;
    loadYCSB(keys + offset, value_lens + offset, offset, FLAGS_load_data_path, FLAGS_load_number);
    offset = FLAGS_run_number/INIT_THREAD_NUMBER * pid;
    readYCSB(keys + offset + FLAGS_load_number, value_lens + offset + FLAGS_load_number, ops + offset, offset, FLAGS_run_data_path, FLAGS_run_number);
}
void initStructure(DRAMManagerNUMA* dramManager,int pid,uint64_t * dataSet, bool * status){
    dramManager->init(dataSet, keyPointer, valuePointer, status);
}

void YCSB(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group threadGroup;
    boost::barrier barrierGroup(FLAGS_thread_number);
    nsTimer initTime,runtime,remainProcessTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();

    auto* processNumber = new int[32];
    auto* processTimer = new double [32];

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&YCSBProcess, dramManagerNUMA, i, boost::ref(barrierGroup),processNumber,processTimer));
    }
    std::cout << "YCSB Begin" << std::endl;
    runtime.start();
    threadGroup.join_all();
    runtime.end();

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&remainProcess, dramManagerNUMA, i, processTimer));
    }
    std::cout << "YCSB Procession" << std::endl;
    remainProcessTime.start();
    threadGroup.join_all();
    remainProcessTime.end();

    double total = FLAGS_run_number;
    double totalTime = 0;
    for(int i = 0;i < FLAGS_thread_number;i++){
        totalTime += processTimer[i];
    }
    totalTime /= FLAGS_thread_number;

    std::cout << "YCSB time: " << (double)runtime.duration() / 1000000 / 1000 << std::endl;
    std::cout << "YCSB total Number: " << total << " YCSB throughput: " << total / totalTime / 1000 /1000<< std::endl;
}

void insert(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group threadGroup;
    boost::barrier barrierGroup(FLAGS_thread_number);
    nsTimer initTime,runtime,remainProcessTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();

    auto* processNumber = new int[32];
    auto* processTimer = new double [32];

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&processYCSB, dramManagerNUMA, i, boost::ref(barrierGroup),processNumber,processTimer,true));
    }
    std::cout << "Insert Begin" << std::endl;
    runtime.start();
    threadGroup.join_all();
    runtime.end();

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&remainProcess, dramManagerNUMA, i, processTimer));
    }
    std::cout << "remain Procession" << std::endl;
    remainProcessTime.start();
    threadGroup.join_all();
    remainProcessTime.end();

    double total = FLAGS_run_number;
    double totalTime = 0;
    for(int i = 0;i < FLAGS_thread_number;i++){
        totalTime += processTimer[i];
    }
    totalTime /= FLAGS_thread_number;

    std::cout << "Insert time: " << (double)runtime.duration() / 1000000 / 1000 << std::endl;
    std::cout << "Insert total Number: " << total << " Insert throughput: " << total / totalTime / 1000 /1000<< std::endl;
}

void search(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group threadGroup;
    boost::barrier barrierGroup(FLAGS_thread_number);
    nsTimer initTime,runtime,remainProcessTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();

    auto* processNumber = new int[32];
    auto* processTimer = new double [32];

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&processYCSB, dramManagerNUMA, i, boost::ref(barrierGroup),processNumber,processTimer,false));
    }
    std::cout << "Search Begin" << std::endl;
    runtime.start();
    threadGroup.join_all();
    runtime.end();

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&remainProcess, dramManagerNUMA, i, processTimer));
    }
    std::cout << "remain Procession" << std::endl;
    remainProcessTime.start();
    threadGroup.join_all();
    remainProcessTime.end();

    double total = FLAGS_run_number;
    double totalTime = 0;
    for(int i = 0;i < FLAGS_thread_number;i++){
        totalTime += processTimer[i];
    }
    totalTime /= FLAGS_thread_number;

    std::cout << "search time: " << (double)runtime.duration() / 1000000 / 1000 << std::endl;
    std::cout << "search total Number: " << total << " search throughput: " << total / totalTime / 1000 /1000<< std::endl;
}

void update(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group threadGroup;
    boost::barrier barrierGroup(FLAGS_thread_number);
    nsTimer initTime,runtime,remainProcessTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();

    auto* processNumber = new int[32];
    auto* processTimer = new double [32];

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&processYCSB, dramManagerNUMA, i, boost::ref(barrierGroup),processNumber,processTimer,true));
    }
    std::cout << "Update Begin" << std::endl;
    runtime.start();
    threadGroup.join_all();
    runtime.end();

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&remainProcess, dramManagerNUMA, i, processTimer));
    }
    std::cout << "remain Procession" << std::endl;
    remainProcessTime.start();
    threadGroup.join_all();
    remainProcessTime.end();

    double total = FLAGS_run_number;
    double totalTime = 0;
    for(int i = 0;i < FLAGS_thread_number;i++){
        totalTime += processTimer[i];
    }
    totalTime /= FLAGS_thread_number;

    std::cout << "Update time: " << (double)runtime.duration() / 1000000 / 1000 << std::endl;
    std::cout << "Update total Number: " << total << " Update throughput: " << total / totalTime / 1000 /1000<< std::endl;
}

void remove(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group threadGroup;
    boost::barrier barrierGroup(FLAGS_thread_number);
    nsTimer initTime,runtime,remainProcessTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();

    auto* processNumber = new int[32];
    auto* processTimer = new double [32];

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&process, dramManagerNUMA, i, boost::ref(barrierGroup),processNumber,processTimer,false));
    }
    std::cout << "delete begin" << std::endl;
    runtime.start();
    threadGroup.join_all();
    runtime.end();

    for (int i = 0; i < FLAGS_thread_number; ++i) {
        threadGroup.create_thread(boost::bind(&removeRemainProcess, dramManagerNUMA, i, processTimer));
    }
    std::cout << "remain Procession" << std::endl;
    remainProcessTime.start();
    threadGroup.join_all();
    remainProcessTime.end();

    double total = FLAGS_run_number;
    double totalTime = 0;
    for(int i = 0;i < FLAGS_thread_number;i++){
        totalTime += processTimer[i];
    }
    totalTime /= FLAGS_thread_number;

    std::cout << "delete time: " << (double)runtime.duration() / 1000000 / 1000 << std::endl;
    std::cout << "delete total Number: " << total << " delete throughput: " << total / totalTime / 1000 /1000<< std::endl;
}

void Portion() {
    keyProcessForHierMulti(keys);
    nsTimer insertTime,searchTime;

    auto * nvmTest = new NVMPortion();
    nvmTest->init("/pmem0/blockHash");
    int n = 10000;
    double array[FLAGS_run_number/n];
    std::ofstream file("output.csv");
    if (!file.is_open()) {
        std::cerr << "can not open file" << std::endl;
        exit(2);
    }

    int counter = 0;
    std::cout << "begin operation" << std::endl;
    insertTime.start();
    int file_counter = 0;
    for(int i = 0;i < FLAGS_run_number;i++){
//        if(i % n == 0 && i != 0){
//            std::cout << "DataSetNumber:" << i << " loadFactor:" << 1.000000 *  i / (nvmTest->nvmBlockManager->start * SEGMENT_DATA_NUMBER) << std::endl;
//            array[file_counter++] = 1.000000 *  i / (nvmTest->nvmBlockManager->start * SEGMENT_DATA_NUMBER);
//        }
        nvmTest->insert(keyPointer[i],valuePointer[i],keys);
    }
    insertTime.end();
//    std::cout << "DataSetNumber:" << FLAGS_run_number + FLAGS_load_number << " loadFactor:" <<1.000000 *  (FLAGS_run_number + FLAGS_load_number) / nvmTest->nvmBlockManager->start / SEGMENT_DATA_NUMBER << std::endl;
    searchTime.start();
    for(int i = 0;i < FLAGS_run_number;i++){
        if(nvmTest->search(keyPointer[i],valuePointer[i],keys)){
            counter++;
        }
    }
    searchTime.end();
    for (int i = 0; i < FLAGS_run_number / n; ++i) {
        file << array[i] << "\n";
    }
    cout << "Insert time:"<< (double)insertTime.duration() / 1000000 / 1000 << endl;
    cout << "Insert throughput:"<< FLAGS_run_number / (double)insertTime.duration() * 1000  << endl;
    cout << "Search time:"<< (double)searchTime.duration() / 1000000 / 1000 << endl;
    cout << "Search throughput:"<< FLAGS_run_number / (double)searchTime.duration() * 1000 << endl;
    cout << "search number:"<< counter << endl;
}

void removeProcess(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, int *removeNumber,double * removeTime){
#ifdef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET((pid + 2), &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    usleep(1000);
    barrier.wait();
    nsTimer ts;
    ts.start();
    int i = pid;
    int counter = 0;
    for(;i < FLAGS_run_number;i += FLAGS_thread_number){
        if(dramManager->remove(keyPointer[i],valuePointer[i])){
            counter++;
        }
    }
    ts.end();
    removeNumber[pid] = counter;
    removeTime[pid] = (double)ts.duration() / 1000000 / 1000;
}
void insertLatency(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, uint32_t * insertTime){
#ifdef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET((pid + 2), &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    usleep(1000);
    barrier.wait();
    int i = pid;
    int counter = 0;
    for(;i < FLAGS_run_number;i += FLAGS_thread_number){
        nsTimer ts;
        ts.start();
        if(dramManager->processRequest(keyPointer[i],valuePointer[i],1)){
            counter++;
        }
        ts.end();
        insertTime[i] = (double)ts.duration();
    }
}
void searchLatency(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, uint32_t * searchTime){
#ifdef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET((pid + 2), &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    usleep(1000);
    barrier.wait();
    int i = pid;
    int counter = 0;
    for(;i < FLAGS_run_number;i += FLAGS_thread_number){
        nsTimer ts;
        ts.start();
        if(dramManager->processRequest(keyPointer[i],valuePointer[i],0)){
            counter++;
        }
        ts.end();
        searchTime[i] = (double)ts.duration();
    }
}
void updateLatency(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, uint32_t * updateTime){
#ifdef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET((pid + 2), &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    usleep(1000);
    barrier.wait();
    int i = pid;
    int counter = 0;
    for(;i < FLAGS_run_number;i += FLAGS_thread_number){
        nsTimer ts;
        ts.start();
        if(dramManager->processRequest(keyPointer[i],valuePointer[i],1)){
            counter++;
        }
        ts.end();
        updateTime[i] = (double)ts.duration();
    }
}
void deleteLatency(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, uint32_t * deleteTime){
#ifdef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET((pid + 2), &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    usleep(1000);
    barrier.wait();
    int i = pid;
    int counter = 0;
    for(;i < FLAGS_run_number;i += FLAGS_thread_number){
        nsTimer ts;
        ts.start();
        if(dramManager->remove(keyPointer[i],valuePointer[i])){
            counter++;
        }
        ts.end();
        deleteTime[i] = (double)ts.duration();
    }
}

void saveDoubleArrayToFile(const std::string& filePath, const uint32_t * array, size_t size) {
    std::ofstream file(filePath, std::ios::out | std::ios::binary);
    if (!file) {
        std::cerr << "Error: Could not open file for writing: " << filePath << std::endl;
        return;
    }
    file.write(reinterpret_cast<const char*>(array), size * sizeof(uint32_t));
    if (file) {
        std::cout << "Array saved successfully to " << filePath << std::endl;
    } else {
        std::cerr << "Error: Writing to file failed." << std::endl;
    }
    file.close();
}
bool readDoubleArrayFromFile(const std::string& filePath, uint32_t* array, size_t size) {
    std::ifstream file(filePath, std::ios::in | std::ios::binary);
    if (!file) {
        std::cerr << "Error: Could not open file for reading: " << filePath << std::endl;
        return false;
    }
    file.read(reinterpret_cast<char*>(array), size * sizeof(uint32_t ));
    if (file) {
        std::cout << "Array read successfully from " << filePath << std::endl;
    } else {
        std::cerr << "Error: Reading from file failed or incomplete." << std::endl;
        return false;
    }
    file.close();
    return true;
}
void computeLatency(uint32_t * list){
    uint64_t sum = 0;
    for(int i = 0;i < FLAGS_run_number;i++){
        sum += list[i];
    }
    double average = (double)sum / FLAGS_run_number;
    vector<double> vec;

    for(int i = 0;i < FLAGS_run_number;i++){
        if(list[i] > average * 2){
            vec.push_back(list[i]);
        }
    }
    cout << "sum:" << sum << endl;
    cout << "average:" << average << endl;
    cout << "Array length: " << vec.size() << endl;
    sort(vec.begin(), vec.end(), greater<>());
    cout << vec[0] << ", " << vec[2000] << ", "  << vec[20000] << ", "  << vec[200000] << ", "  << vec[2000000] << endl;
}
void Latency(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(FLAGS_thread_number);
    nsTimer initTime,insertTime,searchTime,updateTime,searchAllTime,deleteTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    auto* insertTimerList = new uint32_t [FLAGS_run_number];
    auto* updateTimerList = new uint32_t [FLAGS_run_number];
    auto* deleteTimerList = new uint32_t [FLAGS_run_number];
    auto* searchTimerList = new uint32_t [FLAGS_run_number];
    insertThreads.join_all();
    // insert
    for (int i = 0; i < FLAGS_thread_number; ++i) {
        insertThreads.create_thread(boost::bind(&insertLatency, dramManagerNUMA, i, boost::ref(insertBarrier),insertTimerList));
    }
    std::cout << "Insert Begin" << std::endl;
    insertTime.start();
    insertThreads.join_all();
    insertTime.end();
    // search
    int counter = 0;
    boost::thread_group searchThreads;
    boost::barrier searchBarrier(FLAGS_thread_number);
    for (int i = 0; i < FLAGS_thread_number; ++i) {
        searchThreads.create_thread(boost::bind(&searchLatency, dramManagerNUMA, i, boost::ref(searchBarrier),searchTimerList));
    }
    searchTime.start();
    searchThreads.join_all();
    searchTime.end();
    // update
    for (int i = 0; i < FLAGS_thread_number; ++i) {
        insertThreads.create_thread(boost::bind(&updateLatency, dramManagerNUMA, i, boost::ref(insertBarrier),updateTimerList));
    }
    updateTime.start();
    insertThreads.join_all();
    updateTime.end();
    std::cout << "update time: " << (double)updateTime.duration() / 1000000 / 1000 << std::endl;

    // remove
    for (int i = 0; i < FLAGS_thread_number; ++i) {
        insertThreads.create_thread(boost::bind(&deleteLatency, dramManagerNUMA, i, boost::ref(insertBarrier),deleteTimerList));
    }
    deleteTime.start();
    insertThreads.join_all();
    deleteTime.end();
    std::cout << "delete time: " << (double)deleteTime.duration() / 1000000 / 1000 << std::endl;
    saveDoubleArrayToFile("/md0/LatencyTimeOur/insert.bin",insertTimerList,FLAGS_run_number);
    saveDoubleArrayToFile("/md0/LatencyTimeOur/search.bin",searchTimerList,FLAGS_run_number);
    saveDoubleArrayToFile("/md0/LatencyTimeOur/update.bin",updateTimerList,FLAGS_run_number);
    saveDoubleArrayToFile("/md0/LatencyTimeOur/delete.bin",deleteTimerList,FLAGS_run_number);
}
void LatencyInsert(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(FLAGS_thread_number);
    nsTimer initTime,insertTime,searchTime,updateTime,searchAllTime,deleteTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    auto* insertTimerList = new uint32_t [FLAGS_run_number];
    insertThreads.join_all();
    // insert
    for (int i = 0; i < FLAGS_thread_number; ++i) {
        insertThreads.create_thread(boost::bind(&insertLatency, dramManagerNUMA, i, boost::ref(insertBarrier),insertTimerList));
    }
    std::cout << "Insert Begin" << std::endl;
    insertTime.start();
    insertThreads.join_all();
    insertTime.end();
    computeLatency(insertTimerList);
}

void runAndLoadData(DRAMManagerNUMA* dramManagerNUMA){
    Latency(dramManagerNUMA);
    auto* insertTimerList = new uint32_t [FLAGS_run_number];
    auto* updateTimerList = new uint32_t [FLAGS_run_number];
    auto* deleteTimerList = new uint32_t [FLAGS_run_number];
    auto* searchTimerList = new uint32_t [FLAGS_run_number];
    readDoubleArrayFromFile("/md0/LatencyTimeOur/insert.bin",insertTimerList,FLAGS_run_number);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/search.bin",searchTimerList,FLAGS_run_number);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/update.bin",updateTimerList,FLAGS_run_number);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/delete.bin",deleteTimerList,FLAGS_run_number);
    for(int i =0; i< FLAGS_run_number;i++){
        if(deleteTimerList[i] > 1 << 16){
            std::cout << "delete" << deleteTimerList[i] <<std::endl;
        }
        if(searchTimerList[i] > 1 << 16){
            std::cout << "search" << searchTimerList[i] <<std::endl;
        }
        if(insertTimerList[i] > 1 << 16){
            std::cout << "insert" << insertTimerList[i] <<std::endl;
        }
        if(updateTimerList[i] > 1 << 16){
            std::cout << "update" << updateTimerList[i] <<std::endl;
        }
    }
}

void LatencyCompute(){
    auto* insertTimerList = new uint32_t [FLAGS_run_number];
    auto* updateTimerList = new uint32_t [FLAGS_run_number];
    auto* deleteTimerList = new uint32_t [FLAGS_run_number];
    auto* searchTimerList = new uint32_t [FLAGS_run_number];
    readDoubleArrayFromFile("/md0/LatencyTimeOur/insert.bin",insertTimerList,FLAGS_run_number);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/search.bin",searchTimerList,FLAGS_run_number);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/update.bin",updateTimerList,FLAGS_run_number);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/delete.bin",deleteTimerList,FLAGS_run_number);
    computeLatency(insertTimerList);
    computeLatency(updateTimerList);
    computeLatency(deleteTimerList);
    computeLatency(searchTimerList);
}

void generateStrings(const uint64_t* key, size_t keySize, size_t length, char output[][MAX_LENGTH + 1]) {
    std::mt19937 rng(static_cast<unsigned int>(std::time(nullptr)));
    std::uniform_int_distribution<int> dist(0, 15);
    for (size_t i = 0; i < FLAGS_run_number; ++i) {
        std::ostringstream oss;
        oss << std::hex << std::setw(16) << std::setfill('0') << key[i];
        std::string str = oss.str();
        if (str.size() < length) {
            str.append(length - str.size(), '0' + dist(rng));
        } else if (str.size() > length) {
            str = str.substr(0, length);
        }
        std::strncpy(output[i], str.c_str(), length);
        output[i][length] = '\0';
    }
}

void cacheMonitor(){
    keyProcessForHierMulti(keys);
    auto * nvmblock = new NVMBlockManager();
    nsTimer insertTime,searchTime;
    int n = 10000;
    double array[FLAGS_run_number/n];
    std::ofstream file("output.csv");
    if (!file.is_open()) {
        std::cerr << "can not open file" << std::endl;
        exit(2);
    }
    int counter = 0;
    std::cout << "begin operation" << std::endl;
    cout << "Insert time:"<< (double)insertTime.duration() / 1000000 / 1000 << endl;
    cout << "Insert throughput:"<< FLAGS_run_number / (double)insertTime.duration() * 1000  << endl;
    cout << "search number:"<< counter << endl;
    zipfian_key_generator_t* generator_;
    generator_ = new zipfian_key_generator_t(1, 1 << (GENERATE_MSB + SEGMENT_PREFIX),SKEWNESS);
    int retval;
    if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT)
        exit(1);
/* Create an EventSet */
    int EventSet = PAPI_NULL;
    retval = PAPI_create_eventset (&EventSet);
    assert(retval==PAPI_OK);
/* Add an event*/
    retval = PAPI_add_event(EventSet, PAPI_L3_TCM);
    assert(retval==PAPI_OK);
    retval = PAPI_add_event(EventSet, PAPI_L2_TCM);
    assert(retval==PAPI_OK);
    retval = PAPI_add_event(EventSet, PAPI_L1_TCM);
    assert(retval==PAPI_OK);
    if (PAPI_start(EventSet) != PAPI_OK)
        retval = PAPI_start (EventSet);
    assert(retval==PAPI_OK);
    long long values1[3];

    long long values2[3];

    PAPI_read(EventSet, values1);
    assert(retval==PAPI_OK);

/* Stop counting events */
    // Code block to be profiled
    nsTimer ts;
    ts.start();
    uint64_t tmp;
//    testDirectory();
    ts.end();
    printf("Test time %f\n",(double)ts.duration() / 1000000 / 1000);
    retval = PAPI_stop (EventSet,values2);

    assert(retval==PAPI_OK);

    std::cout << "L3 Cache Misses: " << values2[0] - values1[0] << std::endl;
    std::cout << "L2 Cache Misses: " << values2[1] - values1[1] << std::endl;
    std::cout << "L1 Cache Misses: " << values2[2] - values1[2] << std::endl;
}

void start(DRAMManagerNUMA * dramManager) {
    if(strcmp(FLAGS_operations.c_str(), "YCSB") == 0) {
        YCSB(dramManager);
        return ;
    }

    //  Any basic operation but YCSB need to insert key-value items firstly
    insert(dramManager);
    if(strcmp(FLAGS_operations.c_str(), "insert") == 0) {

    }else if(strcmp(FLAGS_operations.c_str(), "search") == 0) {
        search(dramManager);
    }else if(strcmp(FLAGS_operations.c_str(), "update") == 0) {
        update(dramManager);
    }else if(strcmp(FLAGS_operations.c_str(), "delete") == 0) {
        remove(dramManager);
    }
}

int main(int argc, char* argv[]) {
    if(argc!=8&&argc!=1){
        cout<<"Missing parameters!!!!!!!!!!!!!!  "<<argc<<endl;
        throw;
    }
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::cout << "load_number = " << FLAGS_load_number << ", run_number = " << FLAGS_run_number << std::endl;
    std::cout << "load_data_path = " << FLAGS_run_data_path << std::endl;
    std::cout << "run_data_path = " << FLAGS_run_data_path << std::endl;
    std::cout << "pm_path = " << FLAGS_pm_path << std::endl;
    std::cout << "thread_number = " << FLAGS_thread_number << std::endl;
    std::cout << "operations = " << FLAGS_operations << std::endl;

    boost::thread_group insertThreads;
    boost::barrier insertBarrier(FLAGS_thread_number);

    keyPointer = new KeyPointer[FLAGS_run_number + FLAGS_load_number];
    valuePointer = new ValuePointer[FLAGS_run_number + FLAGS_load_number];
    keys = new uint64_t [FLAGS_run_number + FLAGS_load_number];
    value_lens = new uint64_t [FLAGS_run_number + FLAGS_load_number];
    ops = new uint8_t [FLAGS_run_number];
    statu_log = new bool[FLAGS_run_number];

#ifndef RECOVER
    try {
        for (const auto& entry : std::filesystem::directory_iterator("/pmem1")) {
            if (entry.is_regular_file() && entry.path().filename().string().find("blockHash") == 0) {
                std::cout << "Deleting: " << entry.path() << std::endl;
                std::filesystem::remove(entry.path());
            }
        }
        for (const auto& entry : std::filesystem::directory_iterator("/pmem0")) {
            if (entry.is_regular_file() && entry.path().filename().string().find("blockHash") == 0) {
                std::cout << "Deleting: " << entry.path() << std::endl;
                std::filesystem::remove(entry.path());
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
    }
#endif
    auto dramManagerNUMA = new DRAMManagerNUMA();
    printf("Begin readData\n");
    for (int i = 0; i < INIT_THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&readDataSet,i));
    }

    insertThreads.join_all();
    printf("End readData\n");

    initStructure(dramManagerNUMA,0,keys,statu_log);
    start(dramManagerNUMA);
    return 0;
}