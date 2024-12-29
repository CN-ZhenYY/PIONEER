//
// Created by yzy on 24-7-17.
//

#ifndef OPUS_UTIL_H
#define OPUS_UTIL_H
#include <iostream>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <fstream>
#include <vector>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

static const uint64_t kFNVPrime64 = 1099511628211;
#define DUPLICATE
#define RECOVER
#define MAP_SYNC 0x080000
#define MAP_SHARED_VALIDATE 0x03
#define ISNUMA
#define eADR
//#define Binding_threads
#define KEY_LENGTH 64

#define SEGMENT_PREFIX 4
#define SEGMENT_CAPACITY (1 << SEGMENT_PREFIX)
#define BUCKET_CAPACITY 16
#define DRAM_QUEUE_CAPACITY 256
#define THREAD_MSB 5
#define THREAD_NUMBER (1 << THREAD_MSB)
#define INIT_THREAD_NUMBER 32
#define MAX_LENGTH 8
#define SNAPSHOT
#define loadNum 200000000
#define testNum 200000000
#define FREE_BLOCK (1 << 22)
#define STORE 15
#define GENERATE_MSB 24
#define SKEWNESS 0.01

#define NVM_DIRECTORY_DEPTH 20

#define DYNAMIC_SIZE (1 << 28)
#define PERSIST_DEPTH 0
#define CACHELINESIZE 64
#define SKIP_CHAR_NUMBER 30
#define NN 312
#define MM 156
#define MATRIX_A 0xB5026F5AA96619E9ULL
#define UM 0xFFFFFFFF80000000ULL /* Most significant 33 bits */
#define LM 0x7FFFFFFFULL /* Least significant 31 bits */
#define SEGMENT_DATA_NUMBER (SEGMENT_CAPACITY * BUCKET_CAPACITY + STORE)
#define LOAD_DATA_PATH "/md0/ycsb200M/ycsb_load_workloade"
#define RUN_DATA_PATH "/md0/ycsb200M/ycsb_run_workloade"
#define MASK ((0xFFFFFFFF << PERSIST_DEPTH) & 0xFFFFFFFF)
#define ALLOC_SIZE ((size_t)4<<33)
enum { OP_INSERT, OP_READ, OP_DELETE, OP_UPDATE };
using namespace std;


void readYCSB(uint64_t * keys,uint64_t * value_lens,uint8_t * ops){
    std::string load_data = LOAD_DATA_PATH;
    std::ifstream infile_load(load_data.c_str());
    std::string insert("INSERT");
    std::string op;
    uint64_t key;
    uint64_t value_len;
    int count = 0;
    while ((count < loadNum) && infile_load.good()) {
        infile_load >> op >> key >> value_len;
        if (!op.size()) continue;
        if (op.size() != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            std::cout << op << std::endl;
            return;
        }
        keys[count] = key;
        value_lens[count] = value_len;
        count++;
    }
    infile_load.close();
    std::ifstream infile_run(RUN_DATA_PATH);
    std::string remove("REMOVE");
    std::string read("READ");
    std::string update("UPDATE");
    while ((count < loadNum + testNum) && infile_run.good()) {
        infile_run >> op >> key;
        if (op.compare(insert) == 0) {
            infile_run >> value_len;
            ops[count] = OP_INSERT;
            keys[count] = key;
            value_lens[count] = value_len;
        } else if (op.compare(update) == 0) {
            infile_run >> value_len;
            ops[count] = OP_UPDATE;
            keys[count] = key;
            value_lens[count] = value_len;
        } else if (op.compare(read) == 0) {
            ops[count] = OP_READ;
            keys[count] = key;
        } else if (op.compare(remove) == 0) {
            ops[count] = OP_DELETE;
            keys[count] = key;
        } else {
            continue;
        }
        count++;
    }
}
void loadYCSB(uint64_t * keys,uint64_t * value_lens, uint64_t counter){
    std::string load_data = LOAD_DATA_PATH;
    std::ifstream infile_load(load_data.c_str());
    std::string insert("INSERT");
    std::string line;
    std::string op;
    uint64_t key;
    uint64_t value_len;
    int count = 0;
    int p = (int)counter;
    for(int i = 0;i < SKIP_CHAR_NUMBER;i++){
        infile_load.seekg(p, std::ios::cur);
    }

    while ((count < loadNum / INIT_THREAD_NUMBER) && infile_load.good()) {
        infile_load >> op >> key >> value_len;
        if (!op.size()) continue;
        if (op.size() && op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            std::cout << op << std::endl;
            return;
        }
        keys[count] = key;
        value_lens[count] = value_len;
        count++;
    }
}
void readYCSB(uint64_t * keys,uint64_t * value_lens,uint8_t * ops, uint64_t counter){
    std::string op;
    uint64_t key;
    uint64_t value_len;
    std::ifstream infile_run(RUN_DATA_PATH);
    int count = 0;
    int p = (int)counter;

    for(int i = 0;i < SKIP_CHAR_NUMBER;i++){
        infile_run.seekg(p, std::ios::cur);
    }
    std::string insert("INSERT");
    std::string remove("REMOVE");
    std::string read("READ");
    std::string update("UPDATE");
    while ((count < testNum / INIT_THREAD_NUMBER) && infile_run.good()) {
        infile_run >> op >> key >> value_len;
        if (op.compare(insert) == 0) {
            ops[count] = OP_INSERT;
            keys[count] = key;
            value_lens[count] = value_len;
        } else if (op.compare(update) == 0) {
            ops[count] = OP_UPDATE;
            keys[count] = key;
            value_lens[count] = value_len;
        } else if (op.compare(read) == 0) {
            ops[count] = OP_READ;
            keys[count] = key;
            value_lens[count] = value_len;
        } else if (op.compare(remove) == 0) {
            ops[count] = OP_DELETE;
            keys[count] = key;
            value_lens[count] = value_len;
        } else {
            continue;
        }
        count++;
    }
}

inline void mfence(void) {
    asm volatile("mfence":: :"memory");
};
inline void clwb(char *data, size_t len) {
    volatile char *ptr = (char *) ((unsigned long) data & (~(CACHELINESIZE - 1)));
    mfence();
    for (; ptr < data + len; ptr += CACHELINESIZE) {
        asm volatile("clwb %0" : "+m" (*(volatile char *) ptr));
    }
    mfence();
}
class nsTimer
{
public:
    struct timespec t1, t2;
    long long diff, total, count, abnormal, normal;

    nsTimer()
    {
        reset();
    }
    void start()
    {
        clock_gettime(CLOCK_MONOTONIC, &t1);
    }
    long long end(bool flag = false)
    {
        clock_gettime(CLOCK_MONOTONIC, &t2);
        diff = (t2.tv_sec - t1.tv_sec) * 1000000000 +
               (t2.tv_nsec - t1.tv_nsec);
        total += diff;
        count++;
        if (diff > 10000000)
            abnormal++;
        if (diff < 10000)
            normal++;
        return diff;
    }
    long long op_count()
    {
        return count;
    }
    void reset()
    {
        diff = total = count = 0;
    }
    long long duration()
    { // ns
        return total;
    }
    double avg()
    { // ns
        return double(total) / count;
    }
    double abnormal_rate()
    {
        return double(abnormal) / count;
    }
    double normal_rate()
    {
        return double(normal) / count;
    }
};

class Util{
public:
    static void *staticAllocatePMSpace(const string& filePath,uint64_t size){
        int nvm_fd = open(filePath.c_str(), O_CREAT | O_RDWR, 0644);
        if (nvm_fd < 0) {
            std::cerr << "Failed to open file: " << strerror(errno) << std::endl;
            return nullptr;
        }
        size = (size + 255) & ~255;
        if (ftruncate(nvm_fd, size) != 0) {
            std::cerr << "Failed to resize file: " << strerror(errno) << std::endl;
            close(nvm_fd);
            return nullptr;
        }
        void* mapped = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE, nvm_fd, 0);
        if (mapped == MAP_FAILED) {
            std::cerr << "mmap failed: " << strerror(errno) << std::endl;
            close(nvm_fd);
            return nullptr;
        }
//        memset(mapped, 0, size);
        close(nvm_fd);
        return mapped;
    }
    static void *staticRecoveryPMSpace(const string& filePath,uint64_t size){
        int fd = open(filePath.c_str(), O_RDWR);
        if (fd < 0) {
            std::cerr << "Failed to open file: " << strerror(errno) << std::endl;
            return nullptr;
        }

        void* mapped = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE, fd, 0);
        if (mapped == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << strerror(errno) << std::endl;
            close(fd);
            return nullptr;
        }

        close(fd);
        return mapped;

    }



    static uint64_t getMSBs(uint64_t key,unsigned char depth){
        return ( key >> (KEY_LENGTH - depth) ) & ((1 << depth) -1);
    }
    static uint64_t getMidMSBs(uint64_t key,unsigned char depth){
        return ( key >> (KEY_LENGTH - depth - NVM_DIRECTORY_DEPTH - SEGMENT_PREFIX) ) & ((1 << depth) -1);
    }
    static uint64_t getMetaMSBs(uint64_t key,unsigned char depth){
            return ( key >> (KEY_LENGTH - depth - 9) ) & ((1 << depth) -1);
    }
    static uint64_t getLSBs(uint64_t key){
        return key & ((1 << SEGMENT_PREFIX) - 1);
    }

    static uint8_t hashfunc(uint64_t val)
    {
        uint8_t hash = 123;
        int i;
        for (i = 0; i < sizeof(uint64_t); i++)
        {
            uint64_t octet = val & 0x00ff;
            val = val >> 8;

            hash = hash ^ octet;
            hash = hash * kFNVPrime64;
        }
        hash = (hash << 1) + 1;
        return hash;
    }
};

#endif //OPUS_UTIL_H
