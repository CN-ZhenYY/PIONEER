//
// Created by yzy on 24-11-12.
//

#include "PIONEER.h"
#include <cstdlib>
#include <unistd.h>
#include <boost/thread.hpp>
#include "DataGeneration.h"
#include <filesystem>
#include <papi.h>
char output[testNum/testNum][MAX_LENGTH + 1];
uint8_t  *ops;
uint64_t *keys;
uint64_t *value_lens;
KeyPointer* keyPointer;
ValuePointer* valuePointer;


static int mti=NN+1;
static unsigned long long mt[NN];
uint64_t swapBits(uint64_t value, int N, int M, uint64_t mask_N, uint64_t mask_M) {
    uint64_t first_N_bits = value & mask_N;
    uint64_t last_M_bits = value & mask_M;
    first_N_bits <<= 64 - N;
    return first_N_bits | last_M_bits;
}
void init_genrand64(unsigned long long seed)
{
    mt[0] = seed;
    for (mti=1; mti<NN; mti++)
        mt[mti] =  (6364136223846793005ULL * (mt[mti-1] ^ (mt[mti-1] >> 62)) + mti);
}
unsigned long long genrand64_int64(void)
{
    int i;
    unsigned long long x;
    static unsigned long long mag01[2]={0ULL, MATRIX_A};

    if (mti >= NN) { /* generate NN words at one time */

        /* if init_genrand64() has not been called, */
        /* a default initial seed is used     */
        if (mti == NN+1)
            init_genrand64(5489ULL);

        for (i=0;i<NN-MM;i++) {
            x = (mt[i]&UM)|(mt[i+1]&LM);
            mt[i] = mt[i+MM] ^ (x>>1) ^ mag01[(int)(x&1ULL)];
        }
        for (;i<NN-1;i++) {
            x = (mt[i]&UM)|(mt[i+1]&LM);
            mt[i] = mt[i+(MM-NN)] ^ (x>>1) ^ mag01[(int)(x&1ULL)];
        }
        x = (mt[NN-1]&UM)|(mt[0]&LM);
        mt[NN-1] = mt[MM-1] ^ (x>>1) ^ mag01[(int)(x&1ULL)];

        mti = 0;
    }

    x = mt[mti++];

    x ^= (x >> 29) & 0x5555555555555555ULL;
    x ^= (x << 17) & 0x71D67FFFEDA60000ULL;
    x ^= (x << 37) & 0xFFF7EEE000000000ULL;
    x ^= (x >> 43);

    return x;
}

void keyProcessForHier(uint64_t * dataSet,int pid){
    for(uint64_t i = (loadNum + testNum) / THREAD_NUMBER * pid; i < (loadNum + testNum) / THREAD_NUMBER * (pid + 1); i++){
        keyPointer[i].key = dataSet[i];
        valuePointer[i].fingerPrint= Util::hashfunc(dataSet[i]);
        valuePointer[i].ValuePoint = i;
    }
}

void keyProcessForHierMulti(uint64_t * dataSet){
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(THREAD_NUMBER);
    for(int i = 0;i < THREAD_NUMBER;i++){
        insertThreads.create_thread(boost::bind(&keyProcessForHier,dataSet,i));
    }
    insertThreads.join_all();
}

void loadShadow(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier){
#ifndef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(pid+ 2, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    usleep(1000);
    barrier.wait();
    nsTimer ts;
    ts.start();
    int i = pid + loadNum;
    for(;i < loadNum + testNum;i += THREAD_NUMBER){
        dramManager->insert(keyPointer[i],valuePointer[i]);
    }
    ts.end();
}
void insertShadow(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, int *insertNumber,double * insertTime){
#ifndef Binding_threads
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
    for(;i < testNum;i += THREAD_NUMBER){
        if(dramManager->insert(keyPointer[i],valuePointer[i])){
            counter++;
        }
    }
    ts.end();
    insertNumber[pid] = counter;
    insertTime[pid] = (double)ts.duration() / 1000000 / 1000;
    printf("%d Insert time %f Insert Number %d\n",pid,(double)ts.duration() / 1000000 / 1000,insertNumber[pid]);
}
void insertAllSingleShadow(DRAMManagerNUMA* dramManager,int pid,double * insertTime){
    nsTimer ts;
    ts.start();
    dramManager->insertAllSingleThread(pid);
    ts.end();
    insertTime[pid] += (double)ts.duration() / 1000000 / 1000;
//    printf("%d InsertAll time %f\n",pid,(double)ts.duration() / 1000000 / 1000);
}
void searchShadow(DRAMManagerNUMA* dramManager,int pid,boost::barrier& barrier,double * searchTime){
#ifndef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET((pid + 2), &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    nsTimer ts;
    uint64_t counter = 0;
    ts.start();
    for(int i = pid + loadNum;i < testNum + loadNum;i += THREAD_NUMBER){
        counter += dramManager->search(keyPointer[i],valuePointer[i]);
    }
    ts.end();
    searchTime[pid] = (double)ts.duration() / 1000000 / 1000;
    printf("%d Search time %f, Search Number %ld\n",pid,(double)ts.duration() / 1000000 / 1000, counter);
}
void insertALLShadow(DRAMManagerNUMA* dramManager,int pid,double * insertTime){
#ifndef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(pid + 2, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    nsTimer ts;
    ts.start();
    dramManager->insertAll(pid);
    ts.end();
    insertTime[pid] += (double)ts.duration() / 1000000 / 1000;
//    printf("%d InsertAll time %f\n",pid,(double)ts.duration() / 1000000 / 1000);
}
void searchAllShadow(DRAMManagerNUMA* dramManager,int pid,boost::barrier& barrier,double * searchTime){
#ifndef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(pid + 2, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    nsTimer ts;
    int counter = 0;
    ts.start();
    counter += (int)dramManager->searchAll(pid);
    ts.end();
    searchTime[pid] += (double)ts.duration() / 1000000 / 1000;
    printf("%d Search time %f, Search Number %d\n",pid,(double)ts.duration() / 1000000 / 1000, counter);
}
void searchAllSingleShadow(DRAMManagerNUMA* dramManager,int pid,boost::barrier& barrier,double * searchTime){
#ifndef Binding_threads
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(pid + 2, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        printf("Set CPU affinity failue, ERROR:%s\n", strerror(errno));
    }
#endif
    nsTimer ts;
    int counter = 0;
    ts.start();
    counter += (int)dramManager->searchAllSingleThread(pid);
    ts.end();
    searchTime[pid] += (double)ts.duration() / 1000000 / 1000;
    printf("%d Search time %f, Search Number %d\n",pid,(double)ts.duration() / 1000000 / 1000, counter);
}
void YCSBShadow(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, int *insertNumber,double * insertTime){
#ifndef Binding_threads
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
    uint64_t i = pid + loadNum;
    uint64_t counter = 0;

    for(;i < testNum + loadNum;i += THREAD_NUMBER){
        if(ops[i - loadNum]){
            counter += dramManager->search(keyPointer[i],valuePointer[i]);
        }else{
            counter += dramManager->insert(keyPointer[i],valuePointer[i]);
        }
    }
    ts.end();
    insertNumber[pid] = (int)counter;
    insertTime[pid] = (double)ts.duration() / 1000000 / 1000;
//    printf("%d Operation time %f Operation Number %d\n",pid,(double)ts.duration() / 1000000 / 1000,insertNumber[pid]);
}

void readDataSet(int pid){
    int offset = loadNum/INIT_THREAD_NUMBER * pid;
    loadYCSB(keys + offset,value_lens + offset,offset);
    offset = testNum/INIT_THREAD_NUMBER * pid;
    readYCSB(keys + offset + loadNum,value_lens + offset + loadNum,ops + offset,offset);
}
void initStructure(DRAMManagerNUMA* dramManager,int pid,uint64_t * dataSet){
    dramManager->init(dataSet, keyPointer,valuePointer);
}

void YCSBSingleThread(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group YCSBThreads;
    boost::barrier YCSBBarrier(THREAD_NUMBER);
    auto* YCSBNumber = new int[32];
    auto* YCSBTimer = new double [32];
    nsTimer loadTime, initTime, YCSBTime,insertAllTime,searchAllTime;
    // 计时load
    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    if(loadNum != 0){
        for (int i = 0; i < THREAD_NUMBER; ++i) {
            YCSBThreads.create_thread(boost::bind(&loadShadow, dramManagerNUMA, i, boost::ref(YCSBBarrier)));
        }
        std::cout << "Load Begin" << std::endl;
        loadTime.start();
        YCSBThreads.join_all();
        loadTime.end();
        auto* loadALLInsertTime = new double [32];
        std::cout << "Load ALL Begin" << std::endl;
        loadTime.start();
        YCSBThreads.join_all();
        loadTime.end();
    }
    // YCSB
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        YCSBThreads.create_thread(boost::bind(&YCSBShadow, dramManagerNUMA, i, boost::ref(YCSBBarrier),YCSBNumber,YCSBTimer));
    }
    std::cout << "Insert Begin" << std::endl;
    YCSBTime.start();
    YCSBThreads.join_all();
    YCSBTime.end();
    std::cout << "InsertAll time: " << (double)insertAllTime.duration() / 1000000 / 1000 << std::endl;

    double total = 0;
    double totalTime = 0;
    for(int i = 0;i < THREAD_NUMBER;i++){
        total += YCSBNumber[i];
        totalTime += YCSBTimer[i];
    }
    totalTime /= THREAD_NUMBER;
    double loadTimePrint = (double)loadTime.duration() / 1000000 / 1000;
    std::cout << "Load time: " << loadTimePrint << " Load throughput: " << testNum / loadTimePrint / 1000 /1000<< std::endl;
    std::cout << "YCSB time: " << totalTime << " YCSB throughput: " << testNum / totalTime / 1000 /1000<< std::endl;
}
void YCSB(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group YCSBThreads;
    boost::barrier YCSBBarrier(THREAD_NUMBER);
    auto* YCSBNumber = new int[32];
    auto* YCSBTimer = new double [32];
    auto* loadTimer = new double [32];
    nsTimer loadTime,loadInitTime, initTime, YCSBTime,insertAllTime,searchAllTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    // load
    if(loadNum != 0){
        for (int i = 0; i < THREAD_NUMBER; ++i) {
            YCSBThreads.create_thread(boost::bind(&loadShadow, dramManagerNUMA, i, boost::ref(YCSBBarrier)));
        }

        std::cout << "Load Begin" << std::endl;
        loadTime.start();
        YCSBThreads.join_all();
        loadTime.end();
    }
    // YCSB
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        YCSBThreads.create_thread(boost::bind(&YCSBShadow, dramManagerNUMA, i, boost::ref(YCSBBarrier),YCSBNumber,YCSBTimer));
    }
    std::cout << "Insert Begin" << std::endl;
    YCSBTime.start();
    YCSBThreads.join_all();
    YCSBTime.end();
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        YCSBThreads.create_thread(boost::bind(&insertALLShadow, dramManagerNUMA, i,YCSBTimer));
    }
    YCSBThreads.join_all();
    double total = 0;
    double totalTime = 0;
    for(int i = 0;i < THREAD_NUMBER;i++){
        total += YCSBNumber[i];
        totalTime += YCSBTimer[i];
    }
    totalTime /= THREAD_NUMBER;
    double loadTimePrint = (double)loadTime.duration() / 1000000 / 1000;
    std::cout << "Load time: " << loadTimePrint << " Load throughput: " << testNum / loadTimePrint / 1000 /1000<< std::endl;
    std::cout << "YCSB time: " << totalTime << " YCSB throughput: " << testNum / totalTime / 1000 /1000<< std::endl;
}
void insertAndSearch(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(THREAD_NUMBER);
    nsTimer initTime,runtime,searchTime,insertAllTime,searchAllTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    auto* insertNumber = new int[32];
    auto* insertTimer = new double [32];
    auto* searchHierTime = new double [32];
    insertThreads.join_all();
    // insert
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&insertShadow, dramManagerNUMA, i, boost::ref(insertBarrier),insertNumber,insertTimer));
    }
    std::cout << "Insert Begin" << std::endl;
    runtime.start();
    insertThreads.join_all();
    runtime.end();

    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&insertALLShadow, dramManagerNUMA, i, insertTimer));
    }
    std::cout << "InsertAll Begin" << std::endl;
    insertAllTime.start();
    insertThreads.join_all();
    insertAllTime.end();

    // search
    int counter = 0;
    boost::thread_group searchThreads;
    boost::barrier searchBarrier(THREAD_NUMBER);
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        searchThreads.create_thread(boost::bind(&searchShadow, dramManagerNUMA, i, boost::ref(searchBarrier),searchHierTime));
    }
    searchTime.start();
    searchThreads.join_all();
    searchTime.end();

    double total = testNum;
    double totalTime = 0;
    for(int i = 0;i < THREAD_NUMBER;i++){
        totalTime += insertTimer[i];
    }
    totalTime /= THREAD_NUMBER;


    double totalSearchTime = 0;
    for(int i = 0;i < THREAD_NUMBER;i++){
        totalSearchTime += searchHierTime[i];
    }
    totalSearchTime /= THREAD_NUMBER;

    std::cout << "Insert time: " << (double)runtime.duration() / 1000000 / 1000 << std::endl;
    std::cout << "Insert total Number: " << total << " Insert throughput: " << total / totalTime / 1000 /1000<< std::endl;
    std::cout << "Search time: " << (double)searchTime.duration() / 1000000 / 1000 << " search throughput: " << total / totalSearchTime / 1000 /1000<< std::endl;
    std::cout << "Find item: " << counter << std::endl;
}
void testDirectory() {
    keyProcessForHierMulti(keys);
    auto * nvmblock = new NVMBlockManager();
    nsTimer insertTime,searchTime;

    auto * nvmTest = new NVMDirectory(nvmblock, nullptr);
    int n = 10000;
    double array[testNum/n];
    std::ofstream file("output.csv");
    if (!file.is_open()) {
        std::cerr << "can not open file" << std::endl;
        exit(2);
    }

//    auto * nvmTest = new DRAMDirectory();
    int counter = 0;
    std::cout << "begin operation" << std::endl;
    insertTime.start();
    int file_counter = 0;
    for(int i = 0;i < testNum;i++){
//        if(i % n == 0 && i != 0){
//            std::cout << "DataSetNumber:" << i << " loadFactor:" << 1.000000 *  i / (nvmTest->nvmBlockManager->start * SEGMENT_DATA_NUMBER) << std::endl;
//            array[file_counter++] = 1.000000 *  i / (nvmTest->nvmBlockManager->start * SEGMENT_DATA_NUMBER);
//        }
//        nvmTest->insert(keyPointer[i],valuePointer[i],keys);
    }
    insertTime.end();
    std::cout << "DataSetNumber:" << testNum + loadNum << " loadFactor:" <<1.000000 *  (testNum + loadNum) / nvmTest->nvmBlockManager->start / SEGMENT_DATA_NUMBER << std::endl;
    searchTime.start();
//    for(int i = 0;i < testNum;i++){
//        if(nvmTest->search(keyPointer[i],valuePointer[i],keys)){
//            counter++;
//        }
//    }
    searchTime.end();
    for (int i = 0; i < testNum / n; ++i) {
        file << array[i] << "\n";
    }
    cout << "Insert time:"<< (double)insertTime.duration() / 1000000 / 1000 << endl;
    cout << "Insert throughput:"<< testNum / (double)insertTime.duration() * 1000  << endl;
    cout << "Search time:"<< (double)searchTime.duration() / 1000000 / 1000 << endl;
    cout << "Search throughput:"<< testNum / (double)searchTime.duration() * 1000 << endl;
    cout << "search number:"<< counter << endl;
}

//void testManager() {
//    cout << sizeof(NVMBucket) << endl;
//    keyProcessForHierMulti(keys);
//    auto * nvmManager = new NVMManager();
//#ifdef RECOVER
//    nvmManager->loadPointersFromMetaData("/pmem0/blockHash");
//#else
//    nvmManager->savePointersToMetaData("/pmem0/blockHash");
//#endif
//    nvmManager->init();
//    nsTimer insertTime,searchTime;
//
//    int counter = 0;
//    std::cout << "begin operation" << std::endl;
//    insertTime.start();
//    for(int i = 0;i < testNum;i++){
//        nvmManager->insert(keyPointer[i],valuePointer[i],keys);
//    }
//    insertTime.end();
//    searchTime.start();
//    for(int i = 0;i < testNum;i++){
//        if(nvmManager->search(keyPointer[i],valuePointer[i],keys)){
//            counter++;
//        }
//    }
//    searchTime.end();
//    cout << "Insert time:"<< (double)insertTime.duration() / 1000000 / 1000 << endl;
//    cout << "Insert throughput:"<< testNum / (double)insertTime.duration() * 1000  << endl;
//    cout << "Search time:"<< (double)searchTime.duration() / 1000000 / 1000 << endl;
//    cout << "Search throughput:"<< testNum / (double)searchTime.duration() * 1000 << endl;
//    cout << "search number:"<< counter << endl;
//}

void removeShadow(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, int *removeNumber,double * removeTime){
#ifndef Binding_threads
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
    for(;i < testNum;i += THREAD_NUMBER){
        if(dramManager->remove(keyPointer[i],valuePointer[i])){
            counter++;
        }
    }
    ts.end();
    removeNumber[pid] = counter;
    removeTime[pid] = (double)ts.duration() / 1000000 / 1000;
    printf("%d Insert time %f Insert Number %d\n",pid,(double)ts.duration() / 1000000 / 1000,removeNumber[pid]);
}
void InsertSearchUpdateDelete(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(THREAD_NUMBER);
    nsTimer initTime,insertTime,searchTime,updateTime,searchAllTime,deleteTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    auto* insertNumber = new int[32];
    auto* insertTimer = new double [32];
    auto* updateTimer = new double [32];
    auto* deleteTimer = new double [32];
    auto* searchTimer = new double [32];
    insertThreads.join_all();
    // insert
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&insertShadow, dramManagerNUMA, i, boost::ref(insertBarrier),insertNumber,insertTimer));
    }
    std::cout << "Insert Begin" << std::endl;
    insertTime.start();
    insertThreads.join_all();
    insertTime.end();
    // search
    int counter = 0;
    boost::thread_group searchThreads;
    boost::barrier searchBarrier(THREAD_NUMBER);
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        searchThreads.create_thread(boost::bind(&searchShadow, dramManagerNUMA, i, boost::ref(searchBarrier),searchTimer));
    }
    searchTime.start();
    searchThreads.join_all();
    searchTime.end();

    // update
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&insertShadow, dramManagerNUMA, i, boost::ref(insertBarrier),insertNumber,updateTimer));
    }
    updateTime.start();
    insertThreads.join_all();
    updateTime.end();
    std::cout << "update time: " << (double)updateTime.duration() / 1000000 / 1000 << std::endl;

    // remove
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&removeShadow, dramManagerNUMA, i, boost::ref(insertBarrier),insertNumber,deleteTimer));
    }
    deleteTime.start();
    insertThreads.join_all();
    deleteTime.end();
    std::cout << "delete time: " << (double)deleteTime.duration() / 1000000 / 1000 << std::endl;
    double total = testNum;
    double totalInsertTime = 0;
    double totalSearchTime = 0;
    double totalUpdateTime = 0;
    double totalDeleteTime = 0;
    for(int i = 0;i < THREAD_NUMBER;i++){
        totalInsertTime += insertTimer[i];
    }
    totalInsertTime /= THREAD_NUMBER;

    for(int i = 0;i < THREAD_NUMBER;i++){
        totalSearchTime += searchTimer[i];
    }
    totalSearchTime /= THREAD_NUMBER;

    for(int i = 0;i < THREAD_NUMBER;i++){
        totalUpdateTime += updateTimer[i];
    }
    totalUpdateTime /= THREAD_NUMBER;

    for(int i = 0;i < THREAD_NUMBER;i++){
        totalDeleteTime += deleteTimer[i];
    }
    totalDeleteTime /= THREAD_NUMBER;

    std::cout << "Insert time: " << (double)insertTime.duration() / 1000000 / 1000 << " Insert throughput: " << total / totalInsertTime / 1000 /1000<< std::endl;
    std::cout << "Search time: " << (double)searchTime.duration() / 1000000 / 1000 << " Search throughput: " << total / totalSearchTime / 1000 /1000<< std::endl;
    std::cout << "Update time: " << (double)updateTime.duration() / 1000000 / 1000 << " Update throughput: " << total / totalUpdateTime / 1000 /1000<< std::endl;
    std::cout << "Delete time: " << (double)deleteTime.duration() / 1000000 / 1000 << " Delete throughput: " << total / totalDeleteTime / 1000 /1000<< std::endl;
}
void insertLatency(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, uint32_t * insertTime){
#ifndef Binding_threads
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
    for(;i < testNum;i += THREAD_NUMBER){
        nsTimer ts;
        ts.start();
        if(dramManager->insert(keyPointer[i],valuePointer[i])){
            counter++;
        }
        ts.end();
        insertTime[i] = (double)ts.duration();
    }
}
void searchLatency(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, uint32_t * searchTime){
#ifndef Binding_threads
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
    for(;i < testNum;i += THREAD_NUMBER){
        nsTimer ts;
        ts.start();
        if(dramManager->search(keyPointer[i],valuePointer[i])){
            counter++;
        }
        ts.end();
        searchTime[i] = (double)ts.duration();
    }
}
void updateLatency(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, uint32_t * updateTime){
#ifndef Binding_threads
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
    for(;i < testNum;i += THREAD_NUMBER){
        nsTimer ts;
        ts.start();
        if(dramManager->search(keyPointer[i],valuePointer[i])){
            counter++;
        }
        ts.end();
        updateTime[i] = (double)ts.duration();
    }
}
void deleteLatency(DRAMManagerNUMA* dramManager, int pid, boost::barrier& barrier, uint32_t * deleteTime){
#ifndef Binding_threads
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
    for(;i < testNum;i += THREAD_NUMBER){
        nsTimer ts;
        ts.start();
        if(dramManager->search(keyPointer[i],valuePointer[i])){
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
    for(int i = 0;i < testNum;i++){
        sum += list[i];
    }
    double average = (double)sum / testNum;
    vector<double> vec;

    for(int i = 0;i < testNum;i++){
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
    boost::barrier insertBarrier(THREAD_NUMBER);
    nsTimer initTime,insertTime,searchTime,updateTime,searchAllTime,deleteTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    auto* insertTimerList = new uint32_t [testNum];
    auto* updateTimerList = new uint32_t [testNum];
    auto* deleteTimerList = new uint32_t [testNum];
    auto* searchTimerList = new uint32_t [testNum];
    insertThreads.join_all();
    // insert
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&insertLatency, dramManagerNUMA, i, boost::ref(insertBarrier),insertTimerList));
    }
    std::cout << "Insert Begin" << std::endl;
    insertTime.start();
    insertThreads.join_all();
    insertTime.end();
    // search
    int counter = 0;
    boost::thread_group searchThreads;
    boost::barrier searchBarrier(THREAD_NUMBER);
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        searchThreads.create_thread(boost::bind(&searchLatency, dramManagerNUMA, i, boost::ref(searchBarrier),searchTimerList));
    }
    searchTime.start();
    searchThreads.join_all();
    searchTime.end();
    // update
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&updateLatency, dramManagerNUMA, i, boost::ref(insertBarrier),updateTimerList));
    }
    updateTime.start();
    insertThreads.join_all();
    updateTime.end();
    std::cout << "update time: " << (double)updateTime.duration() / 1000000 / 1000 << std::endl;

    // remove
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&deleteLatency, dramManagerNUMA, i, boost::ref(insertBarrier),deleteTimerList));
    }
    deleteTime.start();
    insertThreads.join_all();
    deleteTime.end();
    std::cout << "delete time: " << (double)deleteTime.duration() / 1000000 / 1000 << std::endl;
    saveDoubleArrayToFile("/md0/LatencyTimeOur/insert.bin",insertTimerList,testNum);
    saveDoubleArrayToFile("/md0/LatencyTimeOur/search.bin",searchTimerList,testNum);
    saveDoubleArrayToFile("/md0/LatencyTimeOur/update.bin",updateTimerList,testNum);
    saveDoubleArrayToFile("/md0/LatencyTimeOur/delete.bin",deleteTimerList,testNum);
}
void LatencyInsert(DRAMManagerNUMA* dramManagerNUMA){
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(THREAD_NUMBER);
    nsTimer initTime,insertTime,searchTime,updateTime,searchAllTime,deleteTime;

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    auto* insertTimerList = new uint32_t [testNum];
    insertThreads.join_all();
    // insert
    for (int i = 0; i < THREAD_NUMBER; ++i) {
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
    auto* insertTimerList = new uint32_t [testNum];
    auto* updateTimerList = new uint32_t [testNum];
    auto* deleteTimerList = new uint32_t [testNum];
    auto* searchTimerList = new uint32_t [testNum];
    readDoubleArrayFromFile("/md0/LatencyTimeOur/insert.bin",insertTimerList,testNum);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/search.bin",searchTimerList,testNum);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/update.bin",updateTimerList,testNum);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/delete.bin",deleteTimerList,testNum);
    for(int i =0; i< testNum;i++){
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
    auto* insertTimerList = new uint32_t [testNum];
    auto* updateTimerList = new uint32_t [testNum];
    auto* deleteTimerList = new uint32_t [testNum];
    auto* searchTimerList = new uint32_t [testNum];
    readDoubleArrayFromFile("/md0/LatencyTimeOur/insert.bin",insertTimerList,testNum);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/search.bin",searchTimerList,testNum);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/update.bin",updateTimerList,testNum);
    readDoubleArrayFromFile("/md0/LatencyTimeOur/delete.bin",deleteTimerList,testNum);
    computeLatency(insertTimerList);
    computeLatency(updateTimerList);
    computeLatency(deleteTimerList);
    computeLatency(searchTimerList);
}

void generateStrings(const uint64_t* key, size_t keySize, size_t length, char output[][MAX_LENGTH + 1]) {
    std::mt19937 rng(static_cast<unsigned int>(std::time(nullptr)));
    std::uniform_int_distribution<int> dist(0, 15);
    for (size_t i = 0; i < testNum; ++i) {
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

void testVariableKey(DRAMManagerNUMA * dramManagerNUMA){
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(THREAD_NUMBER);
    nsTimer initTime,insertTime,searchTime,updateTime,searchAllTime,deleteTime;

    generateStrings(keys, testNum, MAX_LENGTH, output);
    dramManagerNUMA->initVariableKey(output,keyPointer,valuePointer);

    initTime.start();
    keyProcessForHierMulti(keys);
    initTime.end();
    auto* insertTimer = new double [32];
    auto* searchTimer = new double [32];
    auto* insertNumber = new int [32];
    insertThreads.join_all();
    // insert
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&insertShadow, dramManagerNUMA, i, boost::ref(insertBarrier),insertNumber,insertTimer));
    }
    std::cout << "Insert Begin" << std::endl;
    insertTime.start();
    insertThreads.join_all();
    insertTime.end();
    // search
    int counter = 0;
    boost::thread_group searchThreads;
    boost::barrier searchBarrier(THREAD_NUMBER);
    for (int i = 0; i < THREAD_NUMBER; ++i) {
        searchThreads.create_thread(boost::bind(&searchShadow, dramManagerNUMA, i, boost::ref(searchBarrier),searchTimer));
    }
    searchTime.start();
    searchThreads.join_all();
    searchTime.end();

    double total = testNum;
    double totalInsertTime = 0;
    double totalSearchTime = 0;
    for(int i = 0;i < THREAD_NUMBER;i++){
        totalInsertTime += insertTimer[i];
    }
    totalInsertTime /= THREAD_NUMBER;

    for(int i = 0;i < THREAD_NUMBER;i++){
        totalSearchTime += searchTimer[i];
    }

    totalSearchTime /= THREAD_NUMBER;

    std::cout << "Insert time: " << (double)insertTime.duration() / 1000000 / 1000 << " Insert throughput: " << total / totalInsertTime / 1000 /1000<< std::endl;
    std::cout << "Search time: " << (double)searchTime.duration() / 1000000 / 1000 << " Search throughput: " << total / totalSearchTime / 1000 /1000<< std::endl;
}



void cacheMonitor(){
    keyProcessForHierMulti(keys);
    auto * nvmblock = new NVMBlockManager();
    nsTimer insertTime,searchTime;

//    auto * nvmTest = new NVMDirectory(nvmblock,22, nullptr);
    int n = 10000;
    double array[testNum/n];
    std::ofstream file("output.csv");
    if (!file.is_open()) {
        std::cerr << "can not open file" << std::endl;
        exit(2);
    }

//    auto * nvmTest = new DRAMDirectory();
    int counter = 0;
    std::cout << "begin operation" << std::endl;
    insertTime.start();
    int file_counter = 0;
    for(int i = 0;i < testNum;i++){
//        nvmTest->insert(keyPointer[i],valuePointer[i],keys);
    }
    insertTime.end();
    cout << "Insert time:"<< (double)insertTime.duration() / 1000000 / 1000 << endl;
    cout << "Insert throughput:"<< testNum / (double)insertTime.duration() * 1000  << endl;
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

//Do something

/* Stop counting events */
    // Code block to be profiled
    nsTimer ts;
    ts.start();
    uint64_t tmp;
    testDirectory();
    ts.end();
    printf("Test time %f\n",(double)ts.duration() / 1000000 / 1000);
    retval = PAPI_stop (EventSet,values2);

    assert(retval==PAPI_OK);

    std::cout << "L3 Cache Misses: " << values2[0] - values1[0] << std::endl;
    std::cout << "L2 Cache Misses: " << values2[1] - values1[1] << std::endl;
    std::cout << "L1 Cache Misses: " << values2[2] - values1[2] << std::endl;
}

int main() {
    boost::thread_group insertThreads;
    boost::barrier insertBarrier(THREAD_NUMBER);

    keyPointer = new KeyPointer[testNum + loadNum];
    valuePointer = new ValuePointer[testNum + loadNum];
    keys = new uint64_t [testNum + loadNum];
    value_lens = new uint64_t [testNum + loadNum];
    ops = new uint8_t [testNum];


#ifdef RECOVER
//    auto dramManagerNUMA = new DRAMManagerNUMA(0);
#else
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


//    printf("Begin initStructure\n");
//    for (int i = 0; i < INIT_THREAD_NUMBER; ++i) {
//        insertThreads.create_thread(boost::bind(&initStructure, dramManagerNUMA, i, keys));
//    }
    printf("Begin readData\n");
    for (int i = 0; i < INIT_THREAD_NUMBER; ++i) {
        insertThreads.create_thread(boost::bind(&readDataSet,i));
    }
    auto dramManagerNUMA = new DRAMManagerNUMA();
    insertThreads.join_all();
    printf("End readData\n");
    initStructure(dramManagerNUMA,0,keys);

//    testManager();

//    cacheMonitor();
//    testVariableKey(dramManagerNUMA);
//    testDirectory();
//    LatencyInsert(dramManagerNUMA);
//    Latency(dramManagerNUMA);
//    LatencyCompute();
//    runAndLoadData(dramManagerNUMA);

    InsertSearchUpdateDelete(dramManagerNUMA);
//    insertAndSearch(dramManagerNUMA);
//    if(THREAD_NUMBER -1){
//        YCSB(dramManagerNUMA);
//    }else{
//        YCSBSingleThread(dramManagerNUMA);
//    }
    return 0;
}