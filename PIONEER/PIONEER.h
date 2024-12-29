//
// Created by dell on 24-11-12.
//

#ifndef PIONEER_H
#define PIONEER_H
#include <unordered_map>
#include <utility>
#include "Util.h"

#define GET_NUMA(x) (x & 1)
#define GET_LSBs(x) ((x >> 1) & ((1ULL << (SEGMENT_PREFIX - 1)) - 1))
#define GET_MSBs(x) (x >> (64 - NVM_DIRECTORY_DEPTH))
#define GET_MidMSBs(x,n) (x >> (64 - NVM_DIRECTORY_DEPTH - n) & ((1 << n) - 1))
union ValuePointer{
    uint64_t value = 0;
    struct {
        uint64_t fingerPrint : 16;
        uint64_t ValuePoint : 48;
    };
};
union KeyPointer{
    uint64_t key = 0;
    struct {
        uint64_t NUMA : 1;
        uint64_t LSBs : SEGMENT_PREFIX;
        uint64_t midMSBs : 64 - SEGMENT_PREFIX - NVM_DIRECTORY_DEPTH - 1;
        uint64_t MSBs : NVM_DIRECTORY_DEPTH;
    };
};
class Entry{
public:
    KeyPointer key;
    ValuePointer value;
};
class DRAMBucket{
public:
//    uint32_t localDepth;
    uint8_t fingerprint[SEGMENT_DATA_NUMBER];
    uint8_t localDepth;
    Entry entry[SEGMENT_DATA_NUMBER];
    DRAMBucket(){
        localDepth = 0;
        cleanFingerprint();
    }
    void cleanFingerprint(){
        for(int i = 0;i < SEGMENT_DATA_NUMBER;i++){
            fingerprint[i] = 0;
        }
    }
    uint64_t find_location(uint64_t pos){
        for(int i = 0; i < BUCKET_CAPACITY; ++i){
            if(!(fingerprint[pos + i] & 1)){
                return pos + i;
            }
        }
        for(int i = 0; i < STORE; ++i){
            if(!(fingerprint[i] & 1)){
                return i;
            }
        }
        return SEGMENT_DATA_NUMBER;
    }
    int search_item_with_fingerprint(uint64_t keyPoint,uint64_t &pos,uint16_t hash,uint64_t* dataSet)
    {
        int i = 0;
        for (; i < BUCKET_CAPACITY; ++i)
        {
            if (fingerprint[pos + i] == hash)
            {
                if (entry[pos + i].key.key == keyPoint)
                {
                    return (int)pos + i;
                }
            }
        }
        for (i = 0; i < STORE; ++i)
        {
            if (fingerprint[i] == hash)
            {
                if (entry[i].key.key == keyPoint)
                {
                    return i;
                }
            }
        }
        return -1;
    }
    int search_item_with_fingerprint_variableKey(uint64_t keyPoint,uint64_t &pos,uint16_t hash,char dataSet[][MAX_LENGTH + 1])
    {
        int i = 0;
        for (; i < BUCKET_CAPACITY; ++i)
        {
            if (fingerprint[pos + i] == hash)
            {
                if (strcmp(dataSet[entry[pos + i].key.key], dataSet[keyPoint]) == 0)
                {
                    return (int)pos + i;
                }
            }
        }
        for (i = 0; i < STORE; ++i)
        {
            if (fingerprint[i] == hash)
            {
                if (strcmp(dataSet[entry[i].key.key], dataSet[keyPoint]) == 0)
                {
                    return i;
                }
            }
        }
        return -1;
    }
    bool modify(uint64_t pos,KeyPointer key){
        entry[pos].key = key;
        return true;
    }
    bool remove(uint64_t pos){
        fingerprint[pos] = 0;
    }
    void copy(DRAMBucket* newSegment,uint8_t localDepth){
        for(uint64_t i = STORE;i < SEGMENT_DATA_NUMBER;i++){
            if(Util::getMidMSBs(entry[i].key.midMSBs,localDepth) & 1){
                newSegment->fingerprint[i] = fingerprint[i];
                newSegment->entry[i].key = entry[i].key;
                newSegment->entry[i].value = entry[i].value;
                fingerprint[i] = 0;
            }
        }
        for(uint64_t i = 0;i < STORE;i++){
            if(Util::getMidMSBs(entry[i].key.midMSBs,localDepth) & 1){
                uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                    if(newSegment->entry[j].key.key == 0){
                        newSegment->fingerprint[j] = fingerprint[i];
                        newSegment->entry[j].key = entry[i].key;
                        newSegment->entry[j].value = entry[i].value;
                        fingerprint[i] = 0;
                        break;
                    }
                }
                if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                    newSegment->fingerprint[i] = fingerprint[i];
                    newSegment->entry[i].key = entry[i].key;
                    newSegment->entry[i].value = entry[i].value;
                    fingerprint[i] = 0;
                }
            }
        }
    }
};
class NVMBucket{
public:
    uint8_t fingerprint[SEGMENT_DATA_NUMBER];
    uint8_t localDepth;
//    uint32_t localDepth;
    Entry entry[SEGMENT_DATA_NUMBER];
    NVMBucket(){
        cleanSegment();
    }
    void cleanSegment(){
        for(int i = 0;i < SEGMENT_DATA_NUMBER;i++){
            fingerprint[i] = 0;
            entry[i].key.key = 0;
            entry[i].value.value = 0;
        }
    }

    int search_item_with_fingerprint_variableKey(uint64_t keyPoint,uint64_t &pos,uint16_t hash,char dataSet[][MAX_LENGTH + 1])
    {
        int i = 0;
        for (; i < BUCKET_CAPACITY; ++i)
        {
            if (fingerprint[pos + i] == hash)
            {
                if (strcmp(dataSet[entry[pos + i].key.key],dataSet[keyPoint]) == 0)
                {
                    return (int)pos + i;
                }
            }
        }
        for (i = 0; i < STORE; ++i)
        {
            if (fingerprint[i] == hash)
            {
                if (strcmp(dataSet[entry[i].key.key],dataSet[keyPoint]) == 0)
                {
                    return i;
                }
            }
        }
        return -1;
    }
    uint64_t find_location(uint64_t pos){
        for(int i = 0; i < BUCKET_CAPACITY; ++i){
            if(!(fingerprint[pos + i] & 1)){
                return pos + i;
            }
        }
        for(int i = 0; i < STORE; ++i){
            if(!(fingerprint[i] & 1)){
                return i;
            }
        }
        return SEGMENT_DATA_NUMBER;
    }
    int search_item_with_fingerprint(uint64_t keyPoint,uint64_t &pos,uint16_t hash,uint64_t* dataSet)
    {
        int i = 0;
        for (; i < BUCKET_CAPACITY; ++i)
        {
            if (fingerprint[pos + i] == hash)
            {
                if (entry[pos + i].key.key == keyPoint)
                {
                    return (int)pos + i;
                }
            }
        }
        for (i = 0; i < STORE; ++i)
        {
            if (fingerprint[i] == hash)
            {
                if (entry[i].key.key == keyPoint)
                {
                    return i;
                }
            }
        }
        return -1;
    }
    bool modify(uint64_t pos,KeyPointer key){
        entry[pos].key = key;
    }
    void copy(NVMBucket* newSegment,uint8_t localDepth){
        for(uint64_t i = STORE;i < SEGMENT_DATA_NUMBER;i++){
            if(Util::getMidMSBs(entry[i].key.midMSBs,localDepth) & 1){
                newSegment->fingerprint[i] = fingerprint[i];
                newSegment->entry[i].key = entry[i].key;
                newSegment->entry[i].value = entry[i].value;
                fingerprint[i] = 0;
            }
        }
        for(uint64_t i = 0;i < STORE;i++){
            if(Util::getMidMSBs(entry[i].key.midMSBs,localDepth) & 1){
                uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                    if(newSegment->entry[j].key.key == 0){
                        newSegment->fingerprint[j] = fingerprint[i];
                        newSegment->entry[j].key = entry[i].key;
                        newSegment->entry[j].value = entry[i].value;
                        fingerprint[i] = 0;
                        break;
                    }
                }
                if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                    newSegment->fingerprint[i] = fingerprint[i];
                    newSegment->entry[i].key = entry[i].key;
                    newSegment->entry[i].value = entry[i].value;
                    fingerprint[i] = 0;
                }
            }
        }
    }
};
class NVMBlockManager{
public:
    uint8_t queueStart = 0;
    uint8_t queueEnd = 0;
    NVMBucket * queue[256];

    uint64_t start = 0;
    NVMBucket freeBucket[FREE_BLOCK];

    NVMBlockManager(){
        start = 0;
        for(auto & i : queue){
            i = nullptr;
        }
    }
    NVMBucket* get(){
        uint8_t number = __sync_fetch_and_add(&queueStart,1);
        if(queue[number] != nullptr){
            NVMBucket * tmp = queue[number];
            queue[number] = nullptr;
            return tmp;
        }else{
            __sync_fetch_and_add(&queueStart,-1);
        }
        return freeBucket + __sync_fetch_and_add(&start,1);
    }
    void remove(NVMBucket* bucket){
        queue[__sync_fetch_and_add(&queueEnd,1)] = bucket;
    }
};
class DynamicDirectory{
public:
    uint64_t number = 0;
    NVMBucket * dynamicDirectory[DYNAMIC_SIZE];
    NVMBucket ** alloc(uint64_t depth){
        uint64_t pos = __sync_fetch_and_add(&number,1 << depth);
        if(pos + (1 << depth) < DYNAMIC_SIZE){
            return dynamicDirectory + pos;
        }else{
            std::cout << "Dynamic alloc error!" << std::endl;
            exit(3);
        }
    }
};
class NVMDirectory{
public:
    uint8_t globalDepth = 0;
    NVMBucket ** bucketPointer;
    NVMBlockManager * nvmBlockManager;
    DynamicDirectory * allocator;
    NVMDirectory(NVMBlockManager * manager,DynamicDirectory* alloc){
        allocator = alloc;
        globalDepth = 0;
        nvmBlockManager = manager;
        bucketPointer = allocator->alloc(globalDepth);
        bucketPointer[0] = manager->get();
    }
    NVMDirectory(NVMBlockManager * manager,uint8_t depth,DynamicDirectory* alloc){
        globalDepth = depth;
        nvmBlockManager = manager;
        bucketPointer = allocator->alloc(globalDepth);
    }
    void initDepth(uint8_t depth){
        globalDepth = depth;
        bucketPointer = allocator->alloc(globalDepth);
        for(int i = 0;i < 1 << globalDepth;i++){
            bucketPointer[i] = nvmBlockManager->get();
        }
    }
    void initDepthByDir(uint8_t depth,DRAMBucket ** dir){
        globalDepth = depth;
        bucketPointer = allocator->alloc(globalDepth);
        for(int i = 0,j = 0;i < 1 << globalDepth;i = j){
            int offset = globalDepth - dir[i]->localDepth;
            bucketPointer[i] = nvmBlockManager->get();
            bucketPointer[i]->localDepth = dir[i]->localDepth;
            for(j = i;j < i + (1 << offset);j++){
                bucketPointer[j] = bucketPointer[i];
            }
        }
    }
    void doubleDirectory(){
        int counter = 0;
        globalDepth = globalDepth + 1;
        auto * newSegmentPointer = allocator->alloc(globalDepth);
        for(int i = 0;i < 1 << globalDepth;i += 2,counter++){
            newSegmentPointer[i] = bucketPointer[counter];
            newSegmentPointer[i + 1] = bucketPointer[counter];
        }
        bucketPointer = newSegmentPointer;
    }
    void bucketSplit(NVMBucket* preSegment,uint64_t MSBs){
        int offset = globalDepth - bucketPointer[MSBs]->localDepth;
        MSBs = (MSBs >> offset) << offset;
        if(bucketPointer[MSBs]->localDepth == globalDepth){
            // double directory
            doubleDirectory();
            MSBs <<= 1;
        }
        // create segment and transfer data
        auto* newSegment = nvmBlockManager->get();
        // reset directory
        bucketPointer[MSBs]->localDepth++;
        newSegment->localDepth = bucketPointer[MSBs]->localDepth;
        preSegment->copy(newSegment,bucketPointer[MSBs]->localDepth);
#ifndef eADR
        clwb((char*)preSegment,sizeof (NVMBucket));
        clwb((char*)newSegment,sizeof (NVMBucket));
#else
        mfence();
#endif
        for(uint64_t i = MSBs + (1 << (globalDepth - bucketPointer[MSBs]->localDepth))
                ;i < MSBs + (1 << (globalDepth - bucketPointer[MSBs]->localDepth + 1));i++){
            bucketPointer[i] = newSegment;
        }
    }
    bool insertVariableKey(KeyPointer key,ValuePointer value,char dataSet[][MAX_LENGTH + 1]){
        while(true) {
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            auto *segment = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
#ifdef DUPLICATE
            int old_slot = segment->search_item_with_fingerprint_variableKey(key.key, position, value.fingerPrint, dataSet);
            if (old_slot >= 0) {
                return segment->modify(old_slot, key);
            }
#endif
            position = segment->find_location(position);
            if(position != SEGMENT_DATA_NUMBER){
                segment->fingerprint[position] = value.fingerPrint;
                segment->entry[position].key = key;
                segment->entry[position].value = value;
                return true;
            }
#ifndef eADR
            clwb((char*)segment->fingerprint + position,sizeof(uint16_t));
            clwb((char*)segment->entry + position,sizeof(Entry));
#else
            mfence();
#endif
            bucketSplit(segment, Util::getMidMSBs(key.midMSBs, globalDepth));
        }
    }
    bool insert(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        while(true) {
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            auto *segment = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
#ifdef DUPLICATE
            int old_slot = segment->search_item_with_fingerprint(key.key, position, value.fingerPrint, dataSet);
            if (old_slot >= 0) {
                return segment->modify(old_slot, key);
            }
#endif
            position = segment->find_location(position);
            if(position != SEGMENT_DATA_NUMBER){
                segment->fingerprint[position] = value.fingerPrint;
                segment->entry[position].key = key;
                segment->entry[position].value = value;
                return true;
            }
#ifndef eADR
            clwb((char*)segment->fingerprint + position,sizeof(uint16_t));
            clwb((char*)segment->entry + position,sizeof(Entry));
#else
            mfence();
#endif
            bucketSplit(segment, Util::getMidMSBs(key.midMSBs, globalDepth));
        }
    }
    bool search(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        if(bucketPointer == nullptr){
            return false;
        }
        auto* dramBucket = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
        if(dramBucket == nullptr){
            return false;
        }
        int old_slot = dramBucket->search_item_with_fingerprint(key.key,position,value.fingerPrint, dataSet);
        if (old_slot >= 0)
        {
            return true;
        }
        return false;
    }
    bool searchVariableKey(KeyPointer key,ValuePointer value,char dataSet[][MAX_LENGTH + 1]){
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        if(bucketPointer == nullptr){
            return false;
        }
        auto* dramBucket = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
        if(dramBucket == nullptr){
            return false;
        }
        int old_slot = dramBucket->search_item_with_fingerprint_variableKey(key.key,position,value.fingerPrint, dataSet);
        if (old_slot >= 0)
        {
            return true;
        }
        return false;
    }
};

class DRAMDirectory{
public:
    uint8_t globalDepth = 0;
    DRAMBucket ** bucketPointer;
    DRAMDirectory(){
        bucketPointer = new DRAMBucket * [1 << globalDepth];
        bucketPointer[0] = new DRAMBucket();
    }
    explicit DRAMDirectory(uint8_t depth){
        globalDepth = depth;
        bucketPointer = new DRAMBucket *[1 << globalDepth];
        for(int i = 0;i < 1 << globalDepth;i++){
            bucketPointer[i] = new DRAMBucket();
        }
    }
    void initDepth(uint8_t depth){
        globalDepth = depth;
        bucketPointer = new DRAMBucket *[1 << globalDepth];
        for(int i = 0;i < 1 << globalDepth;i++){
            bucketPointer[i] = new DRAMBucket();
        }
    }
    void initDepthByDir(uint8_t depth,DRAMBucket ** dir){
        globalDepth = depth;
        bucketPointer = new DRAMBucket * [1 << globalDepth];
        for(int i = 0,j = 0;i < 1 << globalDepth;i = j){
            int offset = globalDepth - dir[i]->localDepth;
            bucketPointer[i] = new DRAMBucket();
            bucketPointer[i]->localDepth = dir[i]->localDepth;
            for(j = i;j < i + (1 << offset);j++){
                bucketPointer[j] = bucketPointer[i];
            }
        }
    }
    void doubleDirectory(){
        int counter = 0;
        globalDepth = globalDepth + 1;
        auto * newSegmentPointer = new DRAMBucket * [1 << globalDepth];
        for(int i = 0;i < 1 << globalDepth;i += 2,counter++){
            newSegmentPointer[i] = bucketPointer[counter];
            newSegmentPointer[i + 1] = bucketPointer[counter];
        }
        bucketPointer = newSegmentPointer;
    }
    void bucketSplit(DRAMBucket* preSegment,uint64_t MSBs){
        int offset = globalDepth - bucketPointer[MSBs]->localDepth;
        MSBs = (MSBs >> offset) << offset;
        if(bucketPointer[MSBs]->localDepth == globalDepth){
            // double directory
            doubleDirectory();
            MSBs <<= 1;
        }
        // create segment and transfer data
        auto* newSegment = new DRAMBucket();
        // reset directory
        bucketPointer[MSBs]->localDepth++;
        newSegment->localDepth = bucketPointer[MSBs]->localDepth;
        preSegment->copy(newSegment,bucketPointer[MSBs]->localDepth);

        for(uint64_t i = MSBs + (1 << (globalDepth - bucketPointer[MSBs]->localDepth))
                ;i < MSBs + (1 << (globalDepth - bucketPointer[MSBs]->localDepth + 1));i++){
            bucketPointer[i] = newSegment;
        }
    }
    bool insert(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        while(true){
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            auto* segment = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
#ifdef DUPLICATE
            int old_slot = segment->search_item_with_fingerprint(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                return segment->modify(old_slot,key);
            }
#endif
            position = segment->find_location(position);
            if(position != SEGMENT_DATA_NUMBER){
                segment->fingerprint[position] = value.fingerPrint;
                segment->entry[position].key = key;
                segment->entry[position].value = value;
                return true;
            }
            return false;
        }
    }
    bool insertVariableKey(KeyPointer key,ValuePointer value,char dataSet[][MAX_LENGTH + 1]){
        while(true){
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            auto* segment = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
#ifdef DUPLICATE
            int old_slot = segment->search_item_with_fingerprint_variableKey(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                return segment->modify(old_slot,key);
            }
#endif
            position = segment->find_location(position);
            if(position != SEGMENT_DATA_NUMBER){
                segment->fingerprint[position] = value.fingerPrint;
                segment->entry[position].key = key;
                segment->entry[position].value = value;
                return true;
            }
            return false;
        }
    }
    bool insertAndSplitVariableKey(KeyPointer key,ValuePointer value,char dataSet[][MAX_LENGTH + 1]){
        while(true){
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            auto* segment = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
#ifdef DUPLICATE
            int old_slot = segment->search_item_with_fingerprint_variableKey(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                return segment->modify(old_slot,key);
            }
#endif
            position = segment->find_location(position);
            if(position != SEGMENT_DATA_NUMBER){
                segment->fingerprint[position] = value.fingerPrint;
                segment->entry[position].key = key;
                segment->entry[position].value = value;
                return true;
            }
            bucketSplit(segment,Util::getMidMSBs(key.midMSBs,globalDepth));
        }
    }
    bool remove(KeyPointer key,ValuePointer value,uint64_t *dataSet) {
        while(true){
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            auto* segment = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
            int old_slot = segment->search_item_with_fingerprint(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                segment->fingerprint[old_slot] = 0;
                return true;
            }
            return false;
        }
    }
    bool insertAndSplit(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        while(true){
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            auto* segment = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
#ifdef DUPLICATE
            int old_slot = segment->search_item_with_fingerprint(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                return segment->modify(old_slot,key);
            }
#endif
            position = segment->find_location(position);
            if(position != SEGMENT_DATA_NUMBER){
                segment->fingerprint[position] = value.fingerPrint;
                segment->entry[position].key = key;
                segment->entry[position].value = value;
                return true;
            }
            bucketSplit(segment,Util::getMidMSBs(key.midMSBs,globalDepth));
        }
    }
    bool search(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        auto* dramBucket = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
        if(dramBucket == nullptr){
            return false;
        }
        int old_slot = dramBucket->search_item_with_fingerprint(key.key,position,value.fingerPrint,dataSet);
        if (old_slot >= 0)
        {
            return true;
        }
        return false;
    }
    bool searchVariableKey(KeyPointer key,ValuePointer value,char dataSet[][MAX_LENGTH + 1]){
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        auto* dramBucket = bucketPointer[Util::getMidMSBs(key.midMSBs,globalDepth)];
        if(dramBucket == nullptr){
            return false;
        }
        int old_slot = dramBucket->search_item_with_fingerprint_variableKey(key.key,position,value.fingerPrint,dataSet);
        if (old_slot >= 0)
        {
            return true;
        }
        return false;
    }
};

class DRAMQueue{
public:
    uint8_t insertEnd[1 << NVM_DIRECTORY_DEPTH];
    uint8_t persistStart[1 << NVM_DIRECTORY_DEPTH];
    Entry entry[(1 << NVM_DIRECTORY_DEPTH) * DRAM_QUEUE_CAPACITY];
    DRAMQueue(){

    };
};
class SearchStore{
public:
    uint8_t *persistStart ;
    uint8_t *insertEnd;
    uint64_t * pointer;
    SearchStore(){
        persistStart = new uint8_t [1 << NVM_DIRECTORY_DEPTH];
        insertEnd = new uint8_t [1 << NVM_DIRECTORY_DEPTH];
        pointer = new uint64_t [(1 << NVM_DIRECTORY_DEPTH) * DRAM_QUEUE_CAPACITY];
    };
};

class DRAMManager{
public:
    string filePath;
    KeyPointer * keyPointerDataSet;
    ValuePointer * valuePointerDataSet;
    uint64_t *dataSet;

    bool * waitForTrans;
    DRAMQueue* dramQueue;
    SearchStore* searchQueue;

    DRAMDirectory ** dramDirectory;
    NVMDirectory ** nvmDirectory;

    NVMBlockManager * blockManager;
    DynamicDirectory * allocDirectory;

    char (*variableDataSet)[MAX_LENGTH + 1];
    explicit DRAMManager(string _filePath, int number) {
        filePath = std::move(_filePath);
        dramQueue = new DRAMQueue();
        searchQueue = new SearchStore();
        dramDirectory = new DRAMDirectory*[1 << NVM_DIRECTORY_DEPTH];
        waitForTrans = new bool[1 << NVM_DIRECTORY_DEPTH];

        savePointersToMetaData();
        for (int i = 0;i < (1 << NVM_DIRECTORY_DEPTH); i ++) {
            dramDirectory[i] = new DRAMDirectory();
            nvmDirectory[i] = new NVMDirectory(blockManager,allocDirectory);
        }
    }
    explicit DRAMManager(string _filePath,int number, bool recover) {
        filePath = std::move(_filePath);
        dramQueue = new DRAMQueue();
        searchQueue = new SearchStore();
        dramDirectory = new DRAMDirectory*[1 << NVM_DIRECTORY_DEPTH];
        waitForTrans = new bool[1 << NVM_DIRECTORY_DEPTH];

        loadPointersFromMetaData();
        for (int i = 0;i < (1 << NVM_DIRECTORY_DEPTH); i ++) {
            dramDirectory[i] = new DRAMDirectory();
            nvmDirectory[i] = new NVMDirectory(blockManager,allocDirectory);
        }
    }

    void savePointersToMetaData() {
        nvmDirectory = static_cast<NVMDirectory **>(Util::staticAllocatePMSpace(filePath + "NVMDirectory",sizeof(NVMDirectory * ) * (1 << NVM_DIRECTORY_DEPTH)));
        blockManager = static_cast<NVMBlockManager *>(Util::staticAllocatePMSpace(filePath + "blockManager",sizeof(NVMBlockManager)));
        allocDirectory = static_cast<DynamicDirectory *>(Util::staticAllocatePMSpace(filePath + "dynamicDirectory",sizeof(DynamicDirectory)));
        std::cout << "save metaData done" << std::endl;

    }

    void loadPointersFromMetaData() {
        nsTimer recoveryTime;
        recoveryTime.start();
        nvmDirectory = static_cast<NVMDirectory **>(Util::staticAllocatePMSpace(filePath + "NVMDirectory",sizeof(NVMDirectory * ) * (1 << NVM_DIRECTORY_DEPTH)));
        blockManager = static_cast<NVMBlockManager *>(Util::staticAllocatePMSpace(filePath + "blockManager",sizeof(NVMBlockManager)));
        allocDirectory = static_cast<DynamicDirectory *>(Util::staticAllocatePMSpace(filePath + "dynamicDirectory",sizeof(DynamicDirectory)));
        recoveryTime.end();
        std::cout << "recovery metaData done, time:" << recoveryTime.duration() << std::endl;
    }
    void init(uint64_t * dataSetName,KeyPointer * keyPointer,ValuePointer * valuePointer){
        dataSet = dataSetName;
        keyPointerDataSet = keyPointer;
        valuePointerDataSet = valuePointer;
    }
    void initVariableKey(char dataSetName[][MAX_LENGTH + 1],KeyPointer * keyPointer,ValuePointer * valuePointer){
        variableDataSet = dataSetName;
        keyPointerDataSet = keyPointer;
        valuePointerDataSet = valuePointer;
    }
    bool insertAllSingleThread(){
        for(uint64_t MSBs = 0;MSBs < (1 << NVM_DIRECTORY_DEPTH);MSBs++) {
            persistAll(MSBs);
        }
    }
    bool insertAll(int pid){
        for(uint64_t MSBs = (1 << NVM_DIRECTORY_DEPTH) / THREAD_NUMBER * 2 * pid;MSBs < (1 << NVM_DIRECTORY_DEPTH) / THREAD_NUMBER * 2 * (pid + 1);MSBs++){
            persistAll(MSBs);
        }
    }

    uint64_t searchAllSingleThread(){
        uint64_t counter_size = 0;
        for(uint64_t MSBs = 0;MSBs < (1 << NVM_DIRECTORY_DEPTH);MSBs++){
            uint8_t number = searchQueue->insertEnd[MSBs];
            counter_size += searchInNVM(MSBs,number);
        }
        return counter_size;
    }
    uint64_t searchAll(int pid){
        uint64_t counter_size = 0;
        for(uint64_t MSBs = (1 << NVM_DIRECTORY_DEPTH) / THREAD_NUMBER * 2 * pid;MSBs < (1 << NVM_DIRECTORY_DEPTH) / THREAD_NUMBER * 2 * (pid + 1);MSBs++){
            uint8_t number = searchQueue->insertEnd[MSBs];
            counter_size += searchInNVM(MSBs,number);
        }
        return counter_size;
    }

    uint64_t searchInNVM(uint64_t MSBs, uint8_t number) {
        uint64_t counter_size = 0;
        for(uint8_t i = searchQueue->persistStart[MSBs]; i != number; i++){
            if(nvmDirectory[MSBs]->search(keyPointerDataSet[searchQueue->pointer[(MSBs * DRAM_QUEUE_CAPACITY) + i]],
                                          valuePointerDataSet[searchQueue->pointer[(MSBs * DRAM_QUEUE_CAPACITY) + i]], dataSet)){
                counter_size ++;
            }
        }
        searchQueue->persistStart[MSBs] = number;
        return counter_size;
    }
    uint64_t searchInNVMVariableKey(uint64_t MSBs, uint8_t number) {
        uint64_t counter_size = 0;
        for(uint8_t i = searchQueue->persistStart[MSBs]; i != number; i++){
            if(nvmDirectory[MSBs]->searchVariableKey(keyPointerDataSet[searchQueue->pointer[(MSBs * DRAM_QUEUE_CAPACITY) + i]],
                                                     valuePointerDataSet[searchQueue->pointer[(MSBs * DRAM_QUEUE_CAPACITY) + i]], variableDataSet)){
                counter_size ++;
            }
        }
        searchQueue->persistStart[MSBs] = number;
        return counter_size;
    }

    uint64_t search(KeyPointer key, ValuePointer value){
        uint64_t MSBs = key.MSBs;
#ifdef SNAPSHOT
        if(dramDirectory[MSBs]->search(key,value,dataSet)){
            return 1;
        }
#else
        if(nvmDirectory[MSBs]->search(key,value,dataSet)){
            return 1;
        }
#endif
        uint8_t number = __sync_fetch_and_add(&searchQueue->insertEnd[MSBs],1);
        searchQueue->pointer[(MSBs * DRAM_QUEUE_CAPACITY) + number] = value.ValuePoint;
        if(number == uint8_t (searchQueue->persistStart[MSBs] + (1 << 7))){
            return searchInNVM(MSBs, number);
        }
        return 0;
    }
    uint64_t searchVariableKey(KeyPointer key, ValuePointer value){
        uint64_t MSBs = key.MSBs;
        if(dramDirectory[MSBs]->searchVariableKey(key,value,variableDataSet)){
            return 1;
        }
        uint8_t number = __sync_fetch_and_add(&searchQueue->insertEnd[MSBs],1);
        searchQueue->pointer[(MSBs * DRAM_QUEUE_CAPACITY) + number] = value.ValuePoint;
        if(number == uint8_t (searchQueue->persistStart[MSBs] + (1 << 7))){
            return searchInNVM(MSBs, number);
        }
        return 0;
    }
    bool insert(KeyPointer key, ValuePointer value){
#ifdef SNAPSHOT
        uint64_t MSBs = key.MSBs;
        while(true){
            if(waitForTrans[MSBs]){
                uint8_t number = __sync_fetch_and_add(&dramQueue->insertEnd[MSBs],1);
                dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].key = key;
                dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].value = value;
                return true;
            }else{
                if(dramDirectory[MSBs]->insert(key,value,dataSet)){
                    return true;
                }else{
                    for(uint64_t k = 0;k < 1 << PERSIST_DEPTH;k++){
                        uint64_t persistMSBs = (MSBs & MASK) + k;
                        if(__sync_bool_compare_and_swap(&waitForTrans[persistMSBs], false, true)){
                            persist(persistMSBs);
                            dramDirectory[persistMSBs]->initDepthByDir(nvmDirectory[persistMSBs]->globalDepth,dramDirectory[persistMSBs]->bucketPointer);
                            __sync_bool_compare_and_swap(&waitForTrans[persistMSBs], true, false);
                            uint8_t i = dramQueue->persistStart[persistMSBs];
                            for(;i != dramQueue->insertEnd[persistMSBs];i++){
                                dramDirectory[persistMSBs]->insert(dramQueue->entry[(persistMSBs * DRAM_QUEUE_CAPACITY) + i].key,
                                                                   dramQueue->entry[(persistMSBs * DRAM_QUEUE_CAPACITY) + i].value,dataSet);
                            }
                            dramQueue->persistStart[persistMSBs] = i;
                        }
                    }
                }
            }
        }
#else
        uint64_t MSBs = key.MSBs;
        uint8_t number = __sync_fetch_and_add(&dramQueue->insertEnd[MSBs],1);
        dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].key = key;
        dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].value = value;
        if(number == uint8_t (dramQueue->persistStart[MSBs] + (1 << 7))){
            if(__sync_bool_compare_and_swap(&waitForTrans[MSBs], false, true)){
                if(nvmDirectory[MSBs] == nullptr){
                    nvmDirectory[MSBs] = new NVMDirectory(blockManager);
                }
                for(uint8_t i = dramQueue->persistStart[MSBs];i != number;i++){
//                    dramDirectory[MSBs]->insertAndSplit(dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + i].key,dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + i].value,dataSet);
                    nvmDirectory[MSBs]->insert(dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + i].key,dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + i].value,dataSet);
                }
//                persist(MSBs);
//                mergePersist(persistMSBs);
//                dramDirectory[MSBs]->initDepth(nvmDirectory[MSBs]->globalDepth);
                dramQueue->persistStart[MSBs] = number;
                __sync_bool_compare_and_swap(&waitForTrans[MSBs], true, false);
            }
        }
        return true;
#endif
    }
    void mergePersist(uint64_t MSBs){
        if(dramDirectory[MSBs] ==nullptr){
            return;
        }
        if(nvmDirectory[MSBs] == nullptr){
            nvmDirectory[MSBs] = new NVMDirectory(blockManager,allocDirectory);
        }
        for(int j = 0; j < 1 << dramDirectory[MSBs]->globalDepth; j += 1 << (dramDirectory[MSBs]->globalDepth - dramDirectory[MSBs]->bucketPointer[j]->localDepth)){
            for(uint64_t i = 0;i < SEGMENT_DATA_NUMBER;i++){
                if(dramDirectory[MSBs]->bucketPointer[j]->entry[i].key.key){
                    nvmDirectory[MSBs]->insert(dramDirectory[MSBs]->bucketPointer[j]->entry[i].key,
                                               dramDirectory[MSBs]->bucketPointer[j]->entry[i].value, dataSet);
                }else{
                    continue;
                }
            }
        }
    }
    void persist(uint64_t MSBs) {
        if(nvmDirectory[MSBs]->bucketPointer != nullptr){
            for(int j = 0; j < 1 << nvmDirectory[MSBs]->globalDepth; j += 1 << (nvmDirectory[MSBs]->globalDepth - nvmDirectory[MSBs]->bucketPointer[j]->localDepth)){
                for(uint64_t i = 0;i < SEGMENT_DATA_NUMBER;i++){
                    if(nvmDirectory[MSBs]->bucketPointer[j]->fingerprint[i]){
                        dramDirectory[MSBs]->insertAndSplit(nvmDirectory[MSBs]->bucketPointer[j]->entry[i].key,
                                                            nvmDirectory[MSBs]->bucketPointer[j]->entry[i].value, dataSet);
                    }else{
                        continue;
                    }
                }
            }
        }else {
            nvmDirectory[MSBs]->bucketPointer = allocDirectory->alloc(dramDirectory[MSBs]->globalDepth);
        }

        nvmDirectory[MSBs]->initDepthByDir(dramDirectory[MSBs]->globalDepth,dramDirectory[MSBs]->bucketPointer);
        for(int j = 0;j < 1 << dramDirectory[MSBs]->globalDepth;j += (1 << (dramDirectory[MSBs]->globalDepth - dramDirectory[MSBs]->bucketPointer[j]->localDepth))){
            memcpy(nvmDirectory[MSBs]->bucketPointer[j],dramDirectory[MSBs]->bucketPointer[j],sizeof(NVMBucket));
        }
    }
    void persistVariableKey(uint64_t MSBs) {
        if(nvmDirectory[MSBs]->bucketPointer != nullptr){
            for(int j = 0; j < 1 << nvmDirectory[MSBs]->globalDepth; j += 1 << (nvmDirectory[MSBs]->globalDepth - nvmDirectory[MSBs]->bucketPointer[j]->localDepth)){
                for(uint64_t i = 0;i < SEGMENT_DATA_NUMBER;i++){
                    if(nvmDirectory[MSBs]->bucketPointer[j]->fingerprint[i]){
                        dramDirectory[MSBs]->insertAndSplitVariableKey(nvmDirectory[MSBs]->bucketPointer[j]->entry[i].key,
                                                                       nvmDirectory[MSBs]->bucketPointer[j]->entry[i].value, variableDataSet);
                    }else{
                        continue;
                    }
                }
            }
        }else {
            nvmDirectory[MSBs]->bucketPointer = allocDirectory->alloc(dramDirectory[MSBs]->globalDepth);
        }

        nvmDirectory[MSBs]->initDepth(dramDirectory[MSBs]->globalDepth);
        for(int j = 0;j < 1 << dramDirectory[MSBs]->globalDepth;j += (1 << (dramDirectory[MSBs]->globalDepth - dramDirectory[MSBs]->bucketPointer[j]->localDepth))){
            for(uint64_t i = 0;i < SEGMENT_DATA_NUMBER;i++){
                nvmDirectory[MSBs]->bucketPointer[j]->entry[i].key = dramDirectory[MSBs]->bucketPointer[j]->entry[i].key;
                nvmDirectory[MSBs]->bucketPointer[j]->entry[i].value = dramDirectory[MSBs]->bucketPointer[j]->entry[i].value;
                nvmDirectory[MSBs]->bucketPointer[j]->fingerprint[i] = dramDirectory[MSBs]->bucketPointer[j]->fingerprint[i];
            }
        }
    }
    void persistAll(uint64_t MSBs) {
        for(uint8_t i = dramQueue->persistStart[MSBs];i != dramQueue->insertEnd[MSBs];i++){
            nvmDirectory[MSBs]->insert(dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + i].key,dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + i].value,dataSet);
        }
        if(dramDirectory[MSBs] ==nullptr){
            return;
        }
        for(int j = 0; j < 1 << dramDirectory[MSBs]->globalDepth; j += 1 << (dramDirectory[MSBs]->globalDepth - dramDirectory[MSBs]->bucketPointer[j]->localDepth)){
            if(nvmDirectory[MSBs]->bucketPointer[j] == nullptr){
                nvmDirectory[MSBs]->bucketPointer[j] = blockManager->get();
            }
            for(uint64_t i = 0;i < SEGMENT_DATA_NUMBER;i++){
                if(dramDirectory[MSBs]->bucketPointer[j]->entry[i].key.key){
                    nvmDirectory[MSBs]->insert(dramDirectory[MSBs]->bucketPointer[j]->entry[i].key,
                                               dramDirectory[MSBs]->bucketPointer[j]->entry[i].value, dataSet);
                }else{
                    continue;
                }
            }
        }
    }
    void persistAllFinal(uint64_t MSBs) {
        if(dramDirectory[MSBs] ==nullptr){
            return;
        }
        for(int j = 0; j < 1 << dramDirectory[MSBs]->globalDepth; j++){
            if(nvmDirectory[MSBs]->bucketPointer[j] == nullptr){
                nvmDirectory[MSBs]->bucketPointer[j] = blockManager->get();
            }
            for(uint64_t i = 0;i < SEGMENT_DATA_NUMBER;i++){
                if(dramDirectory[MSBs]->bucketPointer[j]->entry[i].key.key){
                    nvmDirectory[MSBs]->insert(dramDirectory[MSBs]->bucketPointer[j]->entry[i].key,
                                               dramDirectory[MSBs]->bucketPointer[j]->entry[i].value, dataSet);
                }else{
                    continue;
                }
            }
        }
        dramDirectory[MSBs] = nullptr;
    }
    bool remove(KeyPointer key, ValuePointer value){
        uint64_t MSBs = key.MSBs;
        while(true) {
            if (waitForTrans[MSBs]) {
                uint8_t number = __sync_fetch_and_add(&dramQueue->insertEnd[MSBs], 1);
                dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].key = key;
                dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].value.value = 0;
                return true;
            } else {
                if (dramDirectory[MSBs]->remove(key, value, dataSet)) {
                    return true;
                }
            }
            return false;
        }
    }
    bool insertVariableKey(KeyPointer key, ValuePointer value){
        uint64_t MSBs = key.MSBs;
        while(true){
            if(waitForTrans[MSBs]){
                uint8_t number = __sync_fetch_and_add(&dramQueue->insertEnd[MSBs],1);
                dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].key = key;
                dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].value = value;
                return true;
            }else{
                if(dramDirectory[MSBs]->insertVariableKey(key,value,variableDataSet)){
                    return true;
                }else{
                    if(__sync_bool_compare_and_swap(&waitForTrans[MSBs], false, true)){
                        persistVariableKey(MSBs);
                        dramDirectory[MSBs]->initDepth(nvmDirectory[MSBs]->globalDepth);
                        __sync_bool_compare_and_swap(&waitForTrans[MSBs], true, false);
                        uint8_t i = dramQueue->persistStart[MSBs];
                        for(;i != dramQueue->insertEnd[MSBs];i++){
                            dramDirectory[MSBs]->insertVariableKey(dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + i].key,
                                                                   dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + i].value,variableDataSet);
                        }
                        dramQueue->persistStart[MSBs] = i;
                    }
                }
            }
        }
    }
};
class DRAMManagerNUMA{
public:
    DRAMManager* managerNUMAZero;
    DRAMManager* managerNUMAOne;
#ifndef ISNUMA
    DRAMManagerNUMA(concurrency_fastalloc * concurrencyFastalloc){
        managerNUMAZero = new DRAMManager(concurrencyFastalloc,0);
    }
    void changeDataSet(uint64_t * dataSet){
        managerNUMAZero->dataSet = dataSet;
    }
     void init(uint64_t *dataSetName,KeyPointer * keyPointer,ValuePointer * keyIndexInfo){
        managerNUMAZero->init(dataSetName,keyPointer,keyIndexInfo);
    }
    bool insert(KeyPointer key, ValuePointer value){
        return managerNUMAZero->insert(key,value);
    }
    uint64_t search(KeyPointer key, ValuePointer value){
        return managerNUMAZero->search(key,value);
    }
    bool insertAll(int pid){
        managerNUMAZero->insertAll(pid / 2);
    }
    bool insertAllSingleThread(int pid){
        managerNUMAZero->insertAllSingleThread();
    }
    uint64_t searchAll(int pid){
        return managerNUMAZero->searchAll(pid);
    }
    uint64_t searchAllSingleThread(int pid){
        return managerNUMAZero->searchAllSingleThread();
    }
    bool insertVariableKey(KeyPointer key, ValuePointer value){
        return managerNUMAZero->insertVariableKey(key,value);
    }
    bool remove(KeyPointer key, ValuePointer value){
        return managerNUMAZero->remove(key,value);
    }
    void initVariableKey(char dataSetName[][MAX_LENGTH + 1],KeyPointer * keyPointer,ValuePointer * valuePointer){
        managerNUMAZero->initVariableKey(dataSetName,keyPointer,valuePointer);
    }
#else
    DRAMManagerNUMA(){
        managerNUMAZero = new DRAMManager("/pmem0/blockHash",0);
        managerNUMAOne = new DRAMManager("/pmem1/blockHash",1);
    }
    DRAMManagerNUMA(bool recover){
        managerNUMAZero = new DRAMManager("/pmem0/blockHash",0,recover);
        managerNUMAOne = new DRAMManager("/pmem1/blockHash",1,recover);
    }

    void changeDataSet(uint64_t * dataSet){
        managerNUMAZero->dataSet = dataSet;
        managerNUMAOne->dataSet = dataSet;
    }
    void init(uint64_t *dataSetName,KeyPointer * keyPointer,ValuePointer * valuePointer){
        managerNUMAZero->init(dataSetName,keyPointer,valuePointer);
        managerNUMAOne->init(dataSetName,keyPointer,valuePointer);
    }
    void initVariableKey(char dataSetName[][MAX_LENGTH + 1],KeyPointer * keyPointer,ValuePointer * valuePointer){
        managerNUMAZero->initVariableKey(dataSetName,keyPointer,valuePointer);
        managerNUMAOne->initVariableKey(dataSetName,keyPointer,valuePointer);
    }
    bool insert(KeyPointer key, ValuePointer value){
        if(key.NUMA){
            return managerNUMAOne->insert(key,value);
        }else{
            return managerNUMAZero->insert(key,value);
        }
    }
    bool insertVariableKey(KeyPointer key, ValuePointer value){
        if(key.NUMA){
            return managerNUMAOne->insertVariableKey(key,value);
        }else{
            return managerNUMAZero->insertVariableKey(key,value);
        }
    }
    bool remove(KeyPointer key, ValuePointer value){
        if(key.NUMA){
            return managerNUMAOne->remove(key,value);
        }else{
            return managerNUMAZero->remove(key,value);
        }
    }
    uint64_t search(KeyPointer key, ValuePointer value){
        if(key.NUMA){
            return managerNUMAOne->search(key,value);
        }else{
            return managerNUMAZero->search(key,value);
        }
    }
    uint64_t searchVariableKey(KeyPointer key, ValuePointer value){
        if(key.NUMA){
            return managerNUMAOne->searchVariableKey(key,value);
        }else{
            return managerNUMAZero->searchVariableKey(key,value);
        }
    }
    bool insertAll(int pid){
        if(pid % 2){
            managerNUMAOne->insertAll(pid / 2);
        }else{
            managerNUMAZero->insertAll(pid / 2);
        }
    }
    bool insertAllSingleThread(int pid){
//        managerNUMAOne->insertAllSingleThread();
        managerNUMAZero->insertAllSingleThread();
    }
    uint64_t searchAll(int pid){
        if(pid % 2){
            return managerNUMAOne->searchAll(pid / 2);
        }else{
            return managerNUMAZero->searchAll(pid / 2);
        }
    }
    uint64_t searchAllSingleThread(int pid){
        return managerNUMAOne->searchAllSingleThread() + managerNUMAZero->searchAllSingleThread();
    }

#endif
};
#endif //PIONEER_H
