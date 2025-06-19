//
// Created by yzy on 24-7-17.
//

#ifndef PIONEER_UTIL_H
#define PIONEER_UTIL_H
#include <iostream>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <fstream>
#include <utility>
#include <vector>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <immintrin.h>
#ifdef __GNUC__
#pragma GCC target("avx512bw", "avx512vl", "avx512dq", "avx512f")
#endif

//#define Delta
//#define WithoutStash
#define ISNUMA
#define eADR
//#define Binding_threads
#define SIMD
//#define RECOVER
//#define MERGE
//#define SNAPSHOT
//#define VARIABLE
//#define TESTCACHE

#define SEGMENT_PREFIX 4

#define SEGMENT_CAPACITY (1 << SEGMENT_PREFIX)
#define BUCKET_CAPACITY 16
#define DRAM_QUEUE_PREFIX 8
#define DRAM_QUEUE_CAPACITY 256

#define INIT_THREAD_NUMBER 32
#define MAX_LENGTH 16

#define FREE_BLOCK (1 << 24)
#define STORE 30

#define KEY_LENGTH 64
#define NVM_DIRECTORY_DEPTH 19
#define DYNAMIC_SIZE (1 << 30)

#define Threshold ((1 << 7))
#define SEGMENT_DATA_NUMBER (SEGMENT_CAPACITY * BUCKET_CAPACITY + STORE)
#define BUCKET_DATA_NUMBER (SEGMENT_CAPACITY * BUCKET_CAPACITY)
#define CACHELINESIZE 64
#define SKIP_CHAR_NUMBER 30
#define MAP_SYNC 0x080000
#define MAP_SHARED_VALIDATE 0x03
static const uint64_t kFNVPrime64 = 1099511628211;
#define DISPARITY (1 << (DRAM_QUEUE_PREFIX - SEGMENT_PREFIX))

#define PERSIST_DEPTH 1
#define BASE_THRESHOLD (1 << (8 - PERSIST_DEPTH))
//#define BASE_THRESHOLD 128
#define MASK ((0xFFFFFFFF << PERSIST_DEPTH) & 0xFFFFFFFF)
#define ALLOC_SIZE ((size_t)4<<33)
#define CONSUME_THREAD 1
#define VALUE_SIZE 16
#define preNum 200000000

std::vector<char> default_buffer(VALUE_SIZE, 'X');
const char* DEFAULT = default_buffer.data();
extern bool consumeFlag;

//#define loadNum 000000000

//#define THREAD_MSB 5
//#define THREAD_NUMBER (1 << THREAD_MSB)
//#define LOAD_DATA_PATH "/md0/ycsb200M/ycsb_load_workloada"
//#define RUN_DATA_PATH "/md0/ycsb200M/ycsb_run_workloada"

enum {OP_READ, OP_INSERT, OP_DELETE, OP_UPDATE };
using namespace std;

void loadYCSB(uint64_t * keys,uint64_t * value_lens, uint64_t counter, const std::string& load_path, uint64_t loadNum){
    const std::string& load_data = load_path;
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
void readYCSB(uint64_t * keys,uint64_t * value_lens,uint8_t * ops, uint64_t counter, const std::string& run_path, uint64_t testNum){
    std::string op;
    uint64_t key;
    uint64_t value_len;
    std::ifstream infile_run(run_path);
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

    static void ntw_memcpy(void* dst, const void* src, size_t size) {
        __m128i* s = (__m128i*)src;
        __m128i* d = (__m128i*)dst;

        size_t i;
        for (i = 0; i < size / 16; ++i) {
            __m128i val = _mm_loadu_si128(&s[i]);
            _mm_stream_si128(&d[i], val);  // Non-temporal store
        }

        _mm_sfence();  // Ensure write ordering & persistency
    }

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
    static void prefetch(const void *ptr) {
        typedef struct {
            char x[KEY_LENGTH * 2 * ((1 << 7))];
        } cacheline_t;
        asm volatile("prefetcht0 %0" : : "m"(*(const cacheline_t *)ptr));
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
        return ( key >> (KEY_LENGTH - depth - NVM_DIRECTORY_DEPTH - SEGMENT_PREFIX - 1) ) & ((1 << depth) -1);
    }
    static uint64_t getMetaMSBs(uint64_t key,unsigned char depth){
        return (depth == 0) ? 0 : (key >> (KEY_LENGTH - depth));
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

#endif //PIONEER_UTIL_H
