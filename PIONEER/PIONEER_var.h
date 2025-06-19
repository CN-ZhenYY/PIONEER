//
// Created by yzy on 24-12-12.
//

#ifndef PIONEER_H
#define PIONEER_H

#include <unordered_map>
#include <utility>
#include "Util.h"
thread_local volatile char value_[VALUE_SIZE];
#define GET_NUMA(x) (x & 1)
#define GET_LSBs(x) ((x >> 1) & ((1ULL << (SEGMENT_PREFIX - 1)) - 1))
#define GET_MSBs(x) (x >> (64 - NVM_DIRECTORY_DEPTH))
#define GET_MidMSBs(x,n) (x >> (64 - NVM_DIRECTORY_DEPTH - n) & ((1 << n) - 1))
union staticDirectoryEntry{
    uint64_t directoryEntry = 0;
    struct {
        uint64_t persistStrategy : 1;
        uint64_t globalDepth : 15;
        uint64_t offset : 48;
    };
};
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
    uint16_t localDepth;
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
    void copy(DRAMBucket* newSegment,uint16_t localDepth){
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
    uint16_t localDepth;
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
    uint64_t find_location_without_stash(uint64_t pos){
        for(int i = 0; i < BUCKET_CAPACITY; ++i){
            if(!(fingerprint[pos + i] & 1)){
                return pos + i;
            }
        }
        return SEGMENT_DATA_NUMBER;
    }
    int search_item_with_fingerprint(uint64_t keyPoint,uint64_t &pos,uint16_t hash,uint64_t* dataSet)
    {
#ifdef SIMD
        __m128i _fingerprint = _mm_loadu_si128((const __m128i*)(fingerprint + pos));
        __m128i broadFingerprint = _mm_set1_epi8(hash);
        __m128i cmp = _mm_cmpeq_epi8(_fingerprint, broadFingerprint);
        int mask = _mm_movemask_epi8(cmp);
        while (mask) {
            int i = __builtin_ctz(mask);
            if (entry[pos + i].key.key == keyPoint)
                return (int)pos + i;
            mask &= mask - 1;
        }
        for (int i = 0; i < STORE; ++i)
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
#else
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
#endif
    }
    int search_item_without_stash(uint64_t keyPoint,uint64_t &pos,uint16_t hash,char dataSet[][MAX_LENGTH + 1])
    {
#ifdef SIMD
        __m128i _fingerprint = _mm_loadu_si128((const __m128i*)(fingerprint + pos));
        __m128i broadFingerprint = _mm_set1_epi8(hash);
        __m128i cmp = _mm_cmpeq_epi8(_fingerprint, broadFingerprint);
        int mask = _mm_movemask_epi8(cmp);
        while (mask) {
            int i = __builtin_ctz(mask);
            if (memcmp(dataSet[entry[pos + i].value.ValuePoint],dataSet[keyPoint],MAX_LENGTH + 1) == 0)
                return (int)pos + i;
            mask &= mask - 1;
        }
        return -1;
#else
        int i = 0;
        for (; i < BUCKET_CAPACITY; ++i)
        {
            if (fingerprint[pos + i] == hash)
            {
                if (memcmp(dataSet[entry[pos + i].value.ValuePoint],dataSet[keyPoint],MAX_LENGTH + 1) == 0)
                {
                    return (int)pos + i;
                }
            }
        }
        return -1;
#endif
    }
    bool modify(uint64_t pos,KeyPointer key){
        entry[pos].key = key;
    }
    void copy(NVMBucket* newSegment){
        for(uint64_t i = STORE;i < SEGMENT_DATA_NUMBER;i++){
            if(Util::getMetaMSBs(entry[i].key.key,localDepth) & 1){
                newSegment->fingerprint[i] = fingerprint[i];
                newSegment->entry[i].key = entry[i].key;
                newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                fingerprint[i] = 0;
#ifndef eADR
                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
            }
        }
        for(uint64_t i = 0;i < STORE;i++){
            if(Util::getMetaMSBs(entry[i].key.key,localDepth) & 1){
                uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                    if(newSegment->entry[j].key.key == 0){
                        newSegment->fingerprint[j] = fingerprint[i];
                        newSegment->entry[j].key = entry[i].key;
                        newSegment->entry[j].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->fingerprint + j,sizeof(uint8_t ));
                        clwb((char*)newSegment->entry + j,sizeof(Entry));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        break;
                    }
                }
                if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                    newSegment->fingerprint[i] = fingerprint[i];
                    newSegment->entry[i].key = entry[i].key;
                    newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                    clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                    clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                    fingerprint[i] = 0;
#ifndef eADR
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                }
            }else {
                uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                    if(entry[j].key.key == 0){
                        fingerprint[j] = fingerprint[i];
                        entry[j].key = entry[i].key;
                        entry[j].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->fingerprint + j,sizeof(uint8_t ));
                        clwb((char*)newSegment->entry + j,sizeof(Entry));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        break;
                    }
                }
            }
        }
    }
    void copy_mid(NVMBucket* newSegment){
        for(uint64_t i = STORE;i < SEGMENT_DATA_NUMBER;i++){
            if(Util::getMidMSBs(entry[i].key.midMSBs, localDepth) & 1){
                newSegment->fingerprint[i] = fingerprint[i];
                newSegment->entry[i].key = entry[i].key;
                newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                fingerprint[i] = 0;
#ifndef eADR
                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
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
#ifndef eADR
                        clwb((char*)newSegment->fingerprint + j,sizeof(uint8_t ));
                        clwb((char*)newSegment->entry + j,sizeof(Entry));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        break;
                    }
                }
                if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                    newSegment->fingerprint[i] = fingerprint[i];
                    newSegment->entry[i].key = entry[i].key;
                    newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                    clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                    clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                    fingerprint[i] = 0;
#ifndef eADR
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                }
            }
        }
    }
    void copy_mid_strategy(NVMBucket* newSegment, staticDirectoryEntry * tmpFirstDirectory, char dataSet[][MAX_LENGTH + 1]) {
        if(tmpFirstDirectory->persistStrategy) {
            copy_mid(newSegment);
        }else{
            int indexOld = STORE;
            int indexNew = STORE;
            int counterTrans = 0;
            int counterStash = 0;
            for(int j = STORE;j < SEGMENT_DATA_NUMBER;j += BUCKET_CAPACITY, indexOld = j, indexNew = j) {
                for (int i = j; i < j + BUCKET_CAPACITY; i++) {
                    if (Util::getMidMSBs(entry[i].key.midMSBs, localDepth) & 1) {
                        int k = j;
                        for(;k < indexNew; k++) {
                            if(newSegment->fingerprint[k] == fingerprint[i]) {
                                if(memcmp(dataSet[newSegment->entry[k].value.ValuePoint],dataSet[entry[i].value.ValuePoint],MAX_LENGTH + 1) == 0) {
                                    counterTrans++;
                                    fingerprint[i] = 0;
#ifndef eADR
                                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                    break;
                                }
                            }
                        }
                        if(k == indexNew) {
                            newSegment->fingerprint[indexNew] = fingerprint[i];
                            newSegment->entry[indexNew].key = entry[i].key;
                            newSegment->entry[indexNew].value = entry[i].value;
#ifndef eADR
                            clwb((char*)newSegment->fingerprint + indexNew,sizeof(uint8_t ));
                            clwb((char*)newSegment->entry + indexNew,sizeof(Entry));
#endif
                            indexNew += 1;
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        }
                    } else {
                        int k = j;
                        for(;k < indexOld; k++) {
                            if(fingerprint[k] == fingerprint[i]) {
                                if(memcmp(dataSet[entry[k].value.ValuePoint],dataSet[entry[i].value.ValuePoint],MAX_LENGTH + 1) == 0) {
                                    fingerprint[i] = 0;
                                    counterTrans++;
#ifndef eADR
                                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                    break;
                                }
                            }
                        }
                        if(k == indexOld && indexOld != i) {
                            fingerprint[indexOld] = fingerprint[i];
                            entry[indexOld].key = entry[i].key;
                            entry[indexOld].value = entry[i].value;
#ifndef eADR
                            clwb((char*)fingerprint + indexOld,sizeof(uint8_t ));
                            clwb((char*)entry + indexOld,sizeof(Entry));
#endif
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        }
                        indexOld += 1;
                    }
                }
            }

            for(uint64_t i = 0;i < STORE;i++){
                if(Util::getMidMSBs(entry[i].key.midMSBs,localDepth) & 1){
                    uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                    for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                        if(newSegment->fingerprint[j] == fingerprint[i]) {
                            if(memcmp(dataSet[newSegment->entry[j].value.ValuePoint],dataSet[entry[i].value.ValuePoint],MAX_LENGTH + 1) == 0) {
                                fingerprint[i] = 0;
                                counterTrans++;
                                counterStash++;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                        if((newSegment->fingerprint[j] & 1) != 0){
                            newSegment->fingerprint[j] = fingerprint[i];
                            newSegment->entry[j].key = entry[i].key;
                            newSegment->entry[j].value = entry[i].value;
#ifndef eADR
                            clwb((char*)newSegment->fingerprint + j,sizeof(uint8_t ));
                            clwb((char*)newSegment->entry + j,sizeof(Entry));
#endif
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                    }
                    if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                        newSegment->fingerprint[i] = fingerprint[i];
                        newSegment->entry[i].key = entry[i].key;
                        newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                        clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    }
                }else {
                    uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                    for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                        if(fingerprint[j] == fingerprint[i]) {
                            if(memcmp(dataSet[entry[j].value.ValuePoint],dataSet[entry[i].value.ValuePoint],MAX_LENGTH + 1) == 0) {
                                fingerprint[i] = 0;
                                counterTrans++;
                                counterStash++;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                        if((fingerprint[j] & 1) != 0){
                            fingerprint[j] = fingerprint[i];
                            entry[j].key = entry[i].key;
                            entry[j].value = entry[i].value;
#ifndef eADR
                            clwb((char*)newSegment->fingerprint + j,sizeof(uint8_t ));
                            clwb((char*)newSegment->entry + j,sizeof(Entry));
#endif
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                    }
                    if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                        fingerprint[i] = fingerprint[i];
                        entry[i].key = entry[i].key;
                        entry[i].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                        clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    }
                }
            }
            if(counterTrans) {
                tmpFirstDirectory->persistStrategy = true;
            }
        }
    };
    void merge_and_copy_mid(NVMBucket* newSegment,uint16_t localDepth){
        int indexOld = STORE;
        int indexNew = STORE;
        for(int j = STORE;j < SEGMENT_DATA_NUMBER;j += BUCKET_CAPACITY, indexOld = j, indexNew = j) {
            for (int i = j; i < j + BUCKET_CAPACITY; i++) {
                if (Util::getMidMSBs(entry[i].key.midMSBs, localDepth) & 1) {
                    int k = j;
                    for(;k < indexNew; k++) {
                        if(newSegment->fingerprint[k] == fingerprint[i]) {
                            if(newSegment->entry[k].key.key == entry[i].key.key) {
                                fingerprint[i] = 0;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                    }
                    if(k == indexNew) {
                        newSegment->fingerprint[indexNew] = fingerprint[i];
                        newSegment->entry[indexNew].key = entry[i].key;
                        newSegment->entry[indexNew].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->fingerprint + indexNew,sizeof(uint8_t ));
                        clwb((char*)newSegment->entry + indexNew,sizeof(Entry));
#endif
                        indexNew += 1;
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    }
                } else {
                    int k = j;
                    for(;k < indexOld; k++) {
                        if(fingerprint[k] == fingerprint[i]) {
                            if(entry[k].key.key == entry[i].key.key) {
                                fingerprint[i] = 0;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                    }
                    if(k == indexOld && indexOld != i) {
                        fingerprint[indexOld] = fingerprint[i];
                        entry[indexOld].key = entry[i].key;
                        entry[indexOld].value = entry[i].value;
#ifndef eADR
                        clwb((char*)fingerprint + indexOld,sizeof(uint8_t ));
                        clwb((char*)entry + indexOld,sizeof(Entry));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    }
                    indexOld += 1;
                }
            }
        }

        for(uint64_t i = 0;i < STORE;i++){
            if(Util::getMidMSBs(entry[i].key.midMSBs,localDepth) & 1){
                uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                    if(newSegment->fingerprint[j] == fingerprint[i]) {
                        if(newSegment->entry[j].key.key == entry[i].key.key) {
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                    }
                    if((newSegment->fingerprint[j] & 1) != 0){
                        newSegment->fingerprint[j] = fingerprint[i];
                        newSegment->entry[j].key = entry[i].key;
                        newSegment->entry[j].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->fingerprint + j,sizeof(uint8_t ));
                        clwb((char*)newSegment->entry + j,sizeof(Entry));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        break;
                    }
                }
                if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                    newSegment->fingerprint[i] = fingerprint[i];
                    newSegment->entry[i].key = entry[i].key;
                    newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                    clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                    clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                    fingerprint[i] = 0;
#ifndef eADR
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                }
            }else {
                uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                    if(fingerprint[j] == fingerprint[i]) {
                        if(entry[j].key.key == entry[i].key.key) {
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                    }
                    if((fingerprint[j] & 1) != 0){
                        fingerprint[j] = fingerprint[i];
                        entry[j].key = entry[i].key;
                        entry[j].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->fingerprint + j,sizeof(uint8_t ));
                        clwb((char*)newSegment->entry + j,sizeof(Entry));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        break;
                    }
                }
                if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                    fingerprint[i] = fingerprint[i];
                    entry[i].key = entry[i].key;
                    entry[i].value = entry[i].value;
#ifndef eADR
                    clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                    clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                    fingerprint[i] = 0;
#ifndef eADR
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                }
            }
        }
    }
    void data_aware_strategy(NVMBucket * newSegment, bool * strategy){
        if(*strategy){
            int counterStash = 0;
            for(uint64_t i = STORE;i < SEGMENT_DATA_NUMBER;i++){
                if(Util::getMetaMSBs(entry[i].key.key,localDepth) & 1){
                    newSegment->fingerprint[i] = fingerprint[i];
                    newSegment->entry[i].key = entry[i].key;
                    newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                    clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
                    clwb((char*)newSegment->entry + i,sizeof(Entry));
#endif
                    fingerprint[i] = 0;
#ifndef eADR
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                }
            }

            for(uint64_t i = 0;i < STORE;i++){
                if (Util::getMetaMSBs(entry[i].key.key, localDepth) & 1){
                    uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                    for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                        if((newSegment->fingerprint[j] & 1) == 0){
                            newSegment->fingerprint[j] = fingerprint[i];
                            newSegment->entry[j].key = entry[i].key;
                            newSegment->entry[j].value = entry[i].value;
#ifndef eADR
                            clwb((char*)newSegment->entry + i,sizeof(Entry));
                            clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
#endif
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                        if(newSegment->fingerprint[j] == fingerprint[i]) {
                            if(newSegment->entry[j].key.key == entry[i].key.key) {
                                fingerprint[i] = 0;
                                counterStash++;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                    }
                    if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                        newSegment->fingerprint[i] = fingerprint[i];
                        newSegment->entry[i].key = entry[i].key;
                        newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->entry + i,sizeof(Entry));
                        clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    }
                }else {
                    uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                    for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++) {
                        if ((fingerprint[j] & 1) == 0) {
                            fingerprint[j] = fingerprint[i];
                            entry[j].key = entry[i].key;
                            entry[j].value = entry[i].value;
#ifndef eADR
                            clwb((char*)entry + i,sizeof(Entry));
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                        if (fingerprint[j] == fingerprint[i]) {
                            if (entry[j].key.key == entry[i].key.key) {
                                fingerprint[i] = 0;
                                counterStash++;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                    }
                }
            }
        }else{
            int indexOld = STORE;
            int indexNew = STORE;
            int counterUpdate = 0;
            for(int j = STORE;j < SEGMENT_DATA_NUMBER;j += BUCKET_CAPACITY, indexOld = j, indexNew = j) {
                for (int i = j; i < j + BUCKET_CAPACITY; i++) {
                    if (Util::getMetaMSBs(entry[i].key.key, localDepth) & 1) {
                        int k = j;
                        for(;k < indexNew; k++) {
                            if(newSegment->fingerprint[k] == fingerprint[i]) {
                                if(newSegment->entry[k].key.key == entry[i].key.key) {
                                    fingerprint[i] = 0;
                                    counterUpdate++;
#ifndef eADR
                                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                    break;
                                }
                            }
                        }
                        if(k == indexNew) {
                            newSegment->fingerprint[indexNew] = fingerprint[i];
                            newSegment->entry[indexNew].key = entry[i].key;
                            newSegment->entry[indexNew].value = entry[i].value;
#ifndef eADR
                            clwb((char*)newSegment->entry + indexNew,sizeof(Entry));
                            clwb((char*)newSegment->fingerprint + indexNew,sizeof(uint8_t ));
#endif
                            indexNew += 1;
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        }
                    } else {
                        int k = j;
                        for(;k < indexOld; k++) {
                            if(fingerprint[k] == fingerprint[i]) {
                                if(entry[k].key.key == entry[i].key.key) {
                                    fingerprint[i] = 0;
                                    counterUpdate++;
#ifndef eADR
                                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                    break;
                                }
                            }
                        }
                        if(k == indexOld && indexOld != i) {
                            fingerprint[indexOld] = fingerprint[i];
                            entry[indexOld].key = entry[i].key;
                            entry[indexOld].value = entry[i].value;
#ifndef eADR
                            clwb((char*)entry + indexOld,sizeof(Entry));
                            clwb((char*)fingerprint + indexOld,sizeof(uint8_t ));
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        }
                        indexOld += 1;
                    }
                }
            }


            for(uint64_t i = 0;i < STORE;i++){
                if (Util::getMetaMSBs(entry[i].key.key, localDepth) & 1){
                    uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                    for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                        if((newSegment->fingerprint[j] & 1) == 0){
                            newSegment->fingerprint[j] = fingerprint[i];
                            newSegment->entry[j].key = entry[i].key;
                            newSegment->entry[j].value = entry[i].value;
#ifndef eADR
                            clwb((char*)newSegment->entry + i,sizeof(Entry));
                            clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
#endif
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                        if(newSegment->fingerprint[j] == fingerprint[i]) {
                            if(newSegment->entry[j].key.key == entry[i].key.key) {
                                fingerprint[i] = 0;
                                counterUpdate++;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                    }
                    if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                        newSegment->fingerprint[i] = fingerprint[i];
                        newSegment->entry[i].key = entry[i].key;
                        newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->entry + i,sizeof(Entry));
                        clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    }
                }else {
                    uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                    for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++) {
                        if ((fingerprint[j] & 1) == 0) {
                            fingerprint[j] = fingerprint[i];
                            entry[j].key = entry[i].key;
                            entry[j].value = entry[i].value;
#ifndef eADR
                            clwb((char*)entry + i,sizeof(Entry));
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                        if (fingerprint[j] == fingerprint[i]) {
                            if (entry[j].key.key == entry[i].key.key) {
                                fingerprint[i] = 0;
                                counterUpdate++;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                    }
                }
            }
            if(counterUpdate > SEGMENT_DATA_NUMBER * 0.75){
                *strategy = true;
            }
        }
    };
    void DeDuplication() {
        int indexOld = STORE;
        int indexNew = STORE;
        int counterStash = 0;
        for(int j = STORE;j < SEGMENT_DATA_NUMBER;j += BUCKET_CAPACITY, indexOld = j, indexNew = j) {
            for (int i = j; i < j + BUCKET_CAPACITY; i++) {
                int k = j;
                for(;k < indexOld; k++) {
                    if(fingerprint[k] == fingerprint[i]) {
                        if(entry[k].key.key == entry[i].key.key) {
                            fingerprint[i] = 0;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                    }
                }
                if(k == indexOld && indexOld != i) {
                    fingerprint[indexOld] = fingerprint[i];
                    entry[indexOld].key = entry[i].key;
                    entry[indexOld].value = entry[i].value;
#ifndef eADR
                    clwb((char*)entry + indexOld,sizeof(Entry));
                    clwb((char*)fingerprint + indexOld,sizeof(uint8_t ));
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    fingerprint[i] = 0;
#ifndef eADR
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                }
                indexOld += 1;
            }
        }
        for(uint64_t i = 0;i < STORE;i++){
            uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
            for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++) {
                if ((fingerprint[j] & 1) == 0) {
                    fingerprint[j] = fingerprint[i];
                    entry[j].key = entry[i].key;
                    entry[j].value = entry[i].value;
#ifndef eADR
                    clwb((char*)entry + i,sizeof(Entry));
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    fingerprint[i] = 0;
#ifndef eADR
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    break;
                }
                if (fingerprint[j] == fingerprint[i]) {
                    if (entry[j].key.key == entry[i].key.key) {
                        fingerprint[i] = 0;
                        counterStash++;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        break;
                    }
                }
            }

        }
    }
    void merge_and_copy(NVMBucket* newSegment){
        int indexOld = STORE;
        int indexNew = STORE;
        int counterStash = 0;
        for(int j = STORE;j < SEGMENT_DATA_NUMBER;j += BUCKET_CAPACITY, indexOld = j, indexNew = j) {
            for (int i = j; i < j + BUCKET_CAPACITY; i++) {
                if (Util::getMetaMSBs(entry[i].key.key, localDepth) & 1) {
                    int k = j;
                    for(;k < indexNew; k++) {
                        if(newSegment->fingerprint[k] == fingerprint[i]) {
                            if(newSegment->entry[k].key.key == entry[i].key.key) {
                                fingerprint[i] = 0;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                    }
                    if(k == indexNew) {
                        newSegment->fingerprint[indexNew] = fingerprint[i];
                        newSegment->entry[indexNew].key = entry[i].key;
                        newSegment->entry[indexNew].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->entry + indexNew,sizeof(Entry));
                        clwb((char*)newSegment->fingerprint + indexNew,sizeof(uint8_t ));
#endif
                        indexNew += 1;
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    }
                } else {
                    int k = j;
                    for(;k < indexOld; k++) {
                        if(fingerprint[k] == fingerprint[i]) {
                            if(entry[k].key.key == entry[i].key.key) {
                                fingerprint[i] = 0;
#ifndef eADR
                                clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                                break;
                            }
                        }
                    }
                    if(k == indexOld && indexOld != i) {
                        fingerprint[indexOld] = fingerprint[i];
                        entry[indexOld].key = entry[i].key;
                        entry[indexOld].value = entry[i].value;
#ifndef eADR
                        clwb((char*)entry + indexOld,sizeof(Entry));
                        clwb((char*)fingerprint + indexOld,sizeof(uint8_t ));
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                    }
                    indexOld += 1;
                }
            }
        }
        for(uint64_t i = 0;i < STORE;i++){
            if (Util::getMetaMSBs(entry[i].key.key, localDepth) & 1){
                uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++){
                    if((newSegment->fingerprint[j] & 1) == 0){
                        newSegment->fingerprint[j] = fingerprint[i];
                        newSegment->entry[j].key = entry[i].key;
                        newSegment->entry[j].value = entry[i].value;
#ifndef eADR
                        clwb((char*)newSegment->entry + i,sizeof(Entry));
                        clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        break;
                    }
                    if(newSegment->fingerprint[j] == fingerprint[i]) {
                        if(newSegment->entry[j].key.key == entry[i].key.key) {
                            fingerprint[i] = 0;
                            counterStash++;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                    }
                }
                if(j == (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE){
                    newSegment->fingerprint[i] = fingerprint[i];
                    newSegment->entry[i].key = entry[i].key;
                    newSegment->entry[i].value = entry[i].value;
#ifndef eADR
                    clwb((char*)newSegment->entry + i,sizeof(Entry));
                    clwb((char*)newSegment->fingerprint + i,sizeof(uint8_t ));
#endif
                    fingerprint[i] = 0;
#ifndef eADR
                    clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                }
            }else {
                uint64_t j = entry[i].key.LSBs * BUCKET_CAPACITY + STORE;
                for(;j < (entry[i].key.LSBs + 1) * BUCKET_CAPACITY + STORE;j++) {
                    if ((fingerprint[j] & 1) == 0) {
                        fingerprint[j] = fingerprint[i];
                        entry[j].key = entry[i].key;
                        entry[j].value = entry[i].value;
#ifndef eADR
                        clwb((char*)entry + i,sizeof(Entry));
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        fingerprint[i] = 0;
#ifndef eADR
                        clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                        break;
                    }
                    if (fingerprint[j] == fingerprint[i]) {
                        if (entry[j].key.key == entry[i].key.key) {
                            fingerprint[i] = 0;
                            counterStash++;
#ifndef eADR
                            clwb((char*)fingerprint + i,sizeof(uint8_t ));
#endif
                            break;
                        }
                    }
                }
            }
        }
    }
};
class NVMBlockManager{
public:
    uint8_t queueStart = 0;
    uint8_t queueEnd = 0;
    uint64_t queue[256];

    uint64_t start = 1;
    NVMBucket freeBucket[FREE_BLOCK];

    NVMBlockManager(){
        start = 1;
        for(auto & i : queue){
            i = 0;
        }
    }
    uint64_t get(){
        uint8_t number = __sync_fetch_and_add(&queueStart,1);
        if(queue[number] != 0){
            uint64_t tmp = queue[number];
            queue[number] = 0;
            return tmp;
        }else{
            __sync_fetch_and_add(&queueStart,-1);
        }
        return __sync_fetch_and_add(&start,1);
    }
    void remove(uint64_t bucket){
        queue[__sync_fetch_and_add(&queueEnd,1)] = bucket;
    }
};
class DynamicDirectory{
public:
    uint64_t number = 0;
    uint64_t dynamicDirectory[DYNAMIC_SIZE];
    NVMBucket ** alloc(uint64_t depth){
        uint64_t pos = __sync_fetch_and_add(&number,1 << depth);
        if(pos + (1 << depth) < DYNAMIC_SIZE){
            return nullptr;
//            return dynamicDirectory + pos;
        }else{
            std::cout << "Dynamic alloc error!" << std::endl;
            exit(3);
        }
    }
    uint64_t allocPos(uint64_t depth){
        uint64_t pos = __sync_fetch_and_add(&number,1 << depth);
        if(pos + (1 << depth) < DYNAMIC_SIZE){
            return pos;
        }else{
            std::cout << "Dynamic alloc error!" << std::endl;
            exit(3);
        }
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

            int old_slot = segment->search_item_with_fingerprint(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                return segment->modify(old_slot,key);
            }
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
            int old_slot = segment->search_item_with_fingerprint_variableKey(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                return segment->modify(old_slot,key);
            }
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
            int old_slot = segment->search_item_with_fingerprint_variableKey(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                return segment->modify(old_slot,key);
            }
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
            int old_slot = segment->search_item_with_fingerprint(key.key,position,value.fingerPrint,dataSet);
            if (old_slot >= 0)
            {
                return segment->modify(old_slot,key);
            }
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
    bool type[(1 << NVM_DIRECTORY_DEPTH) * DRAM_QUEUE_CAPACITY];
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

union dynamicDirectoryEntry{
    uint64_t directoryEntry = 0;
    struct {
        uint64_t mergeFlag : 1;
        uint64_t mergeNumber : 15;
        uint64_t offset : 48;
    };
};
class NVMPortion{
public:
    DynamicDirectory * dynamicDirectory;
    NVMBlockManager * segmentManager;
    NVMBucket * segmentBlock;
    uint64_t globalDepth;
    uint64_t * directory;
    bool strategy = false;
    NVMPortion(){

    }
    void init(std::string filePath) {
#ifndef RECOVER
        savePointersToMetaData(filePath);
#else
        loadPointersFromMetaData("/pmem0/blockHash");
#endif
        segmentBlock = segmentManager->freeBucket;

#ifndef RECOVER
        globalDepth = 0;
        directory = dynamicDirectory->dynamicDirectory + dynamicDirectory->allocPos(globalDepth);
        directory[0] = segmentManager->get();
#endif
    }

    bool insert(KeyPointer key, ValuePointer value, uint64_t * dataSet){
        while(true) {
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            auto * segment = segmentBlock + directory[Util::getMetaMSBs(key.key, globalDepth)];
#ifdef Delta
            for(int i = 0; i < BUCKET_CAPACITY; i++){
                if((segment->fingerprint[position + i] & 1) == 0){
                    segment->fingerprint[position + i] = value.fingerPrint;
                    segment->entry[position + i].key = key;
                    segment->entry[position + i].value = value;
#ifndef eADR
                    clwb((char*)segment->fingerprint + position + i,sizeof(uint8_t));
                    clwb((char*)segment->entry + position + i,sizeof(Entry));
#endif
                    return true;
                }
            }
            for(int i = 0; i < STORE; i++){
                if((segment->fingerprint[i] & 1) == 0){
                    segment->fingerprint[i] = value.fingerPrint;
                    segment->entry[i].key = key;
                    segment->entry[i].value = value;
#ifndef eADR
                    clwb((char*)segment->fingerprint + i,sizeof(uint8_t));
                    clwb((char*)segment->entry + i,sizeof(Entry));
#endif
                    return true;
                }
            }
#else

#ifdef WithoutStash
            if(strategy){
                int old_slot = segment->search_item_without_stash(key.key, position, value.fingerPrint, dataSet);
                if (old_slot >= 0) {
                    return segment->modify(old_slot, key);
                }
                position = segment->find_location_without_stash(position);
                if(position != SEGMENT_DATA_NUMBER){
                    segment->fingerprint[position] = value.fingerPrint;
                    segment->entry[position].key = key;
                    segment->entry[position].value = value;
#ifndef eADR
                    clwb((char*)segment->fingerprint + position,sizeof(uint16_t));
            clwb((char*)segment->entry + position,sizeof(Entry));
#endif
                    return true;
                }else {
                    for(int i = 0; i < STORE; ++i){
                        if(!(segment->fingerprint[i] & 1)){
                            segment->fingerprint[i] = value.fingerPrint;
                            segment->entry[i].key = key;
                            segment->entry[i].value = value;
#ifndef eADR
                            clwb((char*)segment->fingerprint + i,sizeof(uint16_t));
            clwb((char*)segment->entry + i,sizeof(Entry));
#endif
                            return true;
                        }
                    }
                }
            }else{
                for(int i = 0; i < BUCKET_CAPACITY; i++){
                    if((segment->fingerprint[position + i] & 1) == 0){
                        segment->fingerprint[position + i] = value.fingerPrint;
                        segment->entry[position + i].key = key;
                        segment->entry[position + i].value = value;
#ifndef eADR
                        clwb((char*)segment->fingerprint + position + i,sizeof(uint8_t));
                    clwb((char*)segment->entry + position + i,sizeof(Entry));
#endif
                        return true;
                    }
                }
                for(int i = 0; i < STORE; i++){
                    if((segment->fingerprint[i] & 1) == 0){
                        segment->fingerprint[i] = value.fingerPrint;
                        segment->entry[i].key = key;
                        segment->entry[i].value = value;
#ifndef eADR
                        clwb((char*)segment->fingerprint + i,sizeof(uint8_t));
                    clwb((char*)segment->entry + i,sizeof(Entry));
#endif
                        return true;
                    }
                }
            }
#else
            int old_slot = segment->search_item_with_fingerprint(key.key, position, value.fingerPrint, dataSet);
            if (old_slot >= 0) {
                return segment->modify(old_slot, key);
            }
            position = segment->find_location(position);
            if(position != SEGMENT_DATA_NUMBER){
                segment->fingerprint[position] = value.fingerPrint;
                segment->entry[position].key = key;
                segment->entry[position].value = value;
#ifndef eADR
                clwb((char*)segment->fingerprint + position,sizeof(uint16_t));
                clwb((char*)segment->entry + position,sizeof(Entry));
#endif
                return true;
            }
#endif
#endif
            bucketSplit(segment, Util::getMetaMSBs(key.key, globalDepth));
        }
    }

    bool remove(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        auto * segment = segmentBlock + directory[Util::getMetaMSBs(key.key, globalDepth)];

        int old_slot = segment->search_item_with_fingerprint(key.key, position, value.fingerPrint, dataSet);
        if (old_slot >= 0) {
            return segment->modify(old_slot, key);
        }else{
            return false;
        }
    }

    void doubleDirectory(){
        int counter = (1 << globalDepth) - 1;
        globalDepth++ ;
        for(int i = (1 << globalDepth) - 1; i >= 0; i -= 2, counter--){
            directory[i] = directory[counter];
            directory[i - 1] = directory[counter];
        }
    }
    void bucketSplit(NVMBucket* preSegment,uint64_t MSBs){
        uint64_t offset = globalDepth - preSegment->localDepth;
        MSBs = (MSBs >> offset) << offset;
        if(preSegment->localDepth == globalDepth){
            // double directory
#ifdef Delta
            if(globalDepth >= Threshold) {
                preSegment->DeDuplication();
                return;
            }
            MSBs <<= 1;
            doubleDirectory();
#else
            MSBs <<= 1;
            doubleDirectory();
#endif
        }
        // create segment and transfer data
        uint64_t newSegment = segmentManager->get();
        // reset directory
        preSegment->localDepth++;
        (segmentBlock + newSegment)->localDepth = preSegment->localDepth;
#ifdef Delta
        preSegment->merge_and_copy(segmentBlock + newSegment);
#else
#ifdef WithoutStash
        preSegment->data_aware_strategy(segmentBlock + newSegment, &strategy);
#else
        preSegment->copy(segmentBlock + newSegment);
#endif
#endif

#ifdef eADR
        clwb((char*)preSegment,sizeof(NVMBucket));
        clwb((char*)(segmentBlock + newSegment),sizeof(NVMBucket));
#endif
        for(uint64_t i = MSBs + (1 << (globalDepth - preSegment->localDepth))
                ;i < MSBs + (1 << (globalDepth - preSegment->localDepth + 1));i++){
            directory[i] = newSegment;
        }
    }

    bool search(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        auto * segment = segmentBlock + directory[Util::getMetaMSBs(key.key, globalDepth)];
#ifdef SIMD
        __m128i fingerprint = _mm_loadu_si128((const __m128i*)(segment->fingerprint + position));
        __m128i broadFingerprint = _mm_set1_epi8(value.fingerPrint);
        __m128i cmp = _mm_cmpeq_epi8(fingerprint, broadFingerprint);
        int mask = _mm_movemask_epi8(cmp);
        while (mask) {
            int i = __builtin_ctz(mask);
            if (segment->entry[position + i].key.key == key.key)
                return true;
            mask &= mask - 1;
        }
#else
        for (int i = 0; i < BUCKET_CAPACITY; ++i)
        {
            if (segment->fingerprint[position + i] == value.fingerPrint)
            {
                if (segment->entry[position + i].key.key == key.key)
                {
                    return true;
                }
            }
        }
#endif
        for (int i = 0; i < STORE; ++i)
        {
            if (segment->fingerprint[i] == value.fingerPrint)
            {
                if (segment->entry[i].key.key == key.key)
                {
                    return true;
                }
            }
        }

        return false;
    }

    void savePointersToMetaData(const string& filePath) {
        segmentManager = static_cast<NVMBlockManager *>(Util::staticAllocatePMSpace(filePath + "blockManager",sizeof(NVMBlockManager)));
        dynamicDirectory = static_cast<DynamicDirectory *>(Util::staticAllocatePMSpace(filePath + "dynamicDirectory",sizeof(DynamicDirectory)));
        segmentManager->start = 1;
        std::cout << "save metaData done" << std::endl;
    }

    void loadPointersFromMetaData(const string& filePath) {
        nsTimer recoveryTime;
        recoveryTime.start();
        segmentManager = static_cast<NVMBlockManager *>(Util::staticRecoveryPMSpace(filePath + "blockManager",sizeof(NVMBlockManager)));
        dynamicDirectory = static_cast<DynamicDirectory *>(Util::staticRecoveryPMSpace(filePath + "dynamicDirectory",sizeof(DynamicDirectory)));
        recoveryTime.end();
        std::cout << "recovery metaData done, time:" << recoveryTime.duration() << std::endl;
    }
};


class NVMManager{
public:
    staticDirectoryEntry * staticDirectory;
    DynamicDirectory * dynamicDirectory;
    NVMBlockManager * segmentManager;
    uint64_t * dynamicBlock;
    NVMBucket * segmentBlock;
    char** valueStore;
    NVMManager(){

    }
    void init(std::string filePath) {
        valueStore = new char*[INIT_THREAD_NUMBER];
        for (int i = 0; i < INIT_THREAD_NUMBER; i++) {
            std::string path = "/pmem" + std::to_string(i % 2) + "/blockHashLog_" + std::to_string(i);
            uint64_t allocSize = (uint64_t )preNum / INIT_THREAD_NUMBER * VALUE_SIZE;
            valueStore[i] = static_cast<char*>(Util::staticAllocatePMSpace(path, allocSize));
        }
#ifndef RECOVER
        savePointersToMetaData(filePath);
#else
        loadPointersFromMetaData(filePath);
#endif
        dynamicBlock = dynamicDirectory->dynamicDirectory;
        segmentBlock = segmentManager->freeBucket;
#ifndef RECOVER
        for(int i = 0;i < 1 << NVM_DIRECTORY_DEPTH;i++){
            staticDirectory[i].offset = dynamicDirectory->allocPos(staticDirectory[i].globalDepth);
            dynamicBlock[staticDirectory[i].offset] = segmentManager->get();
        }
#endif
    }
    bool removeRange(Entry * entry, char dataSet[][MAX_LENGTH + 1], uint8_t start, uint8_t end, uint64_t MSBs, bool * request){
        staticDirectoryEntry * tmpFirstDirectory = staticDirectory + MSBs;
        for(uint8_t i = start; i != end; i++) {
            removeRequest(entry[i].key,entry[i].value,dataSet,tmpFirstDirectory);
        }
    }
    bool processRange(Entry * entry, char dataSet[][MAX_LENGTH + 1], uint8_t start, uint8_t end, uint64_t MSBs, bool * request){
        staticDirectoryEntry * tmpFirstDirectory = staticDirectory + MSBs;
        for(uint8_t i = start; i != end; i++) {
            if (request[i]) {
                insertRequest(entry[i].key,entry[i].value,dataSet,tmpFirstDirectory);
#ifdef eADR
                mfence();
#endif
            } else {
                searchRequest(entry[i].key,entry[i].value,dataSet,tmpFirstDirectory);
            }
        }
    }
    bool directorySnapshotFirstProcess(uint64_t MSBs,uint8_t * fingerprint,Entry * stash, Entry * bucket){
        staticDirectoryEntry * tmpFirstDirectory = staticDirectory + MSBs;
        uint64_t * second_level_directory = dynamicBlock + tmpFirstDirectory->offset;
        auto * segment = segmentBlock + second_level_directory[0];
        memcpy(segment->fingerprint,fingerprint,sizeof(uint8_t) * SEGMENT_DATA_NUMBER);
        memcpy(segment->entry,stash,sizeof(Entry) * STORE);
        memcpy(segment->entry + STORE, bucket, sizeof(Entry) * BUCKET_DATA_NUMBER);
    }
    bool directorySnapshotProcess(uint64_t MSBs,uint8_t * fingerprint,Entry * stash, Entry * bucket){
        staticDirectoryEntry * tmpFirstDirectory = staticDirectory + MSBs;
        uint64_t * second_level_directory = dynamicBlock + tmpFirstDirectory->offset;
        auto * segment = segmentBlock + second_level_directory[0];
        memcpy(segment->fingerprint,fingerprint,sizeof(uint8_t) * SEGMENT_DATA_NUMBER);
        memcpy(segment->entry,stash,sizeof(Entry) * STORE);
        memcpy(segment->entry + STORE, bucket, sizeof(Entry) * BUCKET_DATA_NUMBER);
    }
    bool removeRequest(KeyPointer key, ValuePointer value, char dataSet[][MAX_LENGTH + 1], staticDirectoryEntry * tmpFirstDirectory){
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        uint64_t * second_level_directory = dynamicBlock + tmpFirstDirectory->offset;
        auto * segment = segmentBlock + second_level_directory[Util::getMidMSBs(key.midMSBs,tmpFirstDirectory->globalDepth)];
        int old_slot = segment->search_item_without_stash(key.key, position, value.fingerPrint, dataSet);
        if (old_slot >= 0) {
            segment->entry[old_slot].value.fingerPrint = 0;
            return true;
        }
        position = segment->find_location(position);
        if(position != SEGMENT_DATA_NUMBER){
            segment->entry[position].value.fingerPrint = 0;
            return true;
        }
        return false;
    }
    bool searchRequest(KeyPointer key, ValuePointer value, char dataSet[][MAX_LENGTH + 1], staticDirectoryEntry * tmpFirstDirectory){
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        uint64_t * second_level_directory = dynamicBlock + tmpFirstDirectory->offset;
        auto * segment = segmentBlock + second_level_directory[Util::getMidMSBs(key.midMSBs,tmpFirstDirectory->globalDepth)];
#ifdef SIMD
        __m128i fingerprint = _mm_loadu_si128((const __m128i*)(segment->fingerprint + position));
        __m128i broadFingerprint = _mm_set1_epi8(value.fingerPrint);
        __m128i cmp = _mm_cmpeq_epi8(fingerprint, broadFingerprint);
        int mask = _mm_movemask_epi8(cmp);

        while (mask) {
            int i = __builtin_ctz(mask);
            if (memcmp(dataSet[segment->entry[position + i].value.ValuePoint], dataSet[value.ValuePoint],MAX_LENGTH + 1) == 0) {
                memcpy((void *) value_, valueStore[value.ValuePoint % INIT_THREAD_NUMBER] + (value.ValuePoint / INIT_THREAD_NUMBER * VALUE_SIZE), VALUE_SIZE);
                return true;
            }

            mask &= mask - 1;
        }
#else
        for (int i = 0; i < BUCKET_CAPACITY; ++i)
        {
            if (segment->fingerprint[position + i] == value.fingerPrint)
            {
                if (memcmp(dataSet[segment->entry[position + i].value.ValuePoint], dataSet[value.ValuePoint],MAX_LENGTH + 1) == 0)
                {
                    return true;
                }
            }
        }
#endif
        for (int i = 0; i < STORE; ++i)
        {
            if (segment->fingerprint[i] == value.fingerPrint)
            {
                if (memcmp(dataSet[segment->entry[i].value.ValuePoint], dataSet[value.ValuePoint],MAX_LENGTH + 1) == 0)
                {
                    memcpy((void *) value_, valueStore[value.ValuePoint%INIT_THREAD_NUMBER] + (value.ValuePoint/INIT_THREAD_NUMBER * VALUE_SIZE), VALUE_SIZE);
                    return true;
                }
            }
        }
        return false;
    }
    bool insertRequest(KeyPointer key, ValuePointer value, char dataSet[][MAX_LENGTH + 1], staticDirectoryEntry * tmpFirstDirectory){
        while(true) {
            uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
            uint64_t * second_level_directory = dynamicBlock + tmpFirstDirectory->offset;
            auto * segment = segmentBlock + second_level_directory[Util::getMidMSBs(key.midMSBs,tmpFirstDirectory->globalDepth)];
            if(tmpFirstDirectory->persistStrategy) {
                int old_slot = segment->search_item_without_stash(value.ValuePoint, position, value.fingerPrint, dataSet);
                if (old_slot >= 0) {
                    return segment->modify(old_slot, key);
                }
                position = segment->find_location_without_stash(position);
                if(position != SEGMENT_DATA_NUMBER){
                    segment->fingerprint[position] = value.fingerPrint;
                    segment->entry[position].key = key;
                    segment->entry[position].value = value;
#ifndef eADR
                    clwb((char*)segment->fingerprint + position,sizeof(uint16_t));
                    clwb((char*)segment->entry + position,sizeof(Entry));
#endif
                    return true;
                }else {
                    for(int i = 0; i < STORE; ++i){
                        if(!(segment->fingerprint[i] & 1)){
                            segment->fingerprint[i] = value.fingerPrint;
                            segment->entry[i].key = key;
                            segment->entry[i].value = value;
#ifndef eADR
                            clwb((char*)segment->fingerprint + i,sizeof(uint16_t));
                            clwb((char*)segment->entry + i,sizeof(Entry));
#endif
                            return true;
                        }
                    }
                }
            }
            else {
                for(int i = 0; i < BUCKET_CAPACITY; i++){
                    if((segment->fingerprint[position + i] & 1) == 0){
                        segment->fingerprint[position + i] = value.fingerPrint;
                        segment->entry[position + i].key = key;
                        segment->entry[position + i].value = value;
#ifndef eADR
                        clwb((char*)segment->fingerprint + position,sizeof(uint8_t ));
                        clwb((char*)segment->entry + position,sizeof(Entry));
#endif
                        return true;
                    }
                }
                for(int i = 0; i < STORE; i++){
                    if((segment->fingerprint[i] & 1) == 0){
                        segment->fingerprint[i] = value.fingerPrint;
                        segment->entry[i].key = key;
                        segment->entry[i].value = value;
#ifndef eADR
                        clwb((char*)segment->fingerprint,sizeof(uint8_t ));
                        clwb((char*)segment->entry,sizeof(Entry));
#endif
                        return true;
                    }
                }
            }
            bucketSplit(segment, Util::getMidMSBs(key.midMSBs, tmpFirstDirectory->globalDepth), tmpFirstDirectory, dataSet);
        }
    }


    bool remove(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        staticDirectoryEntry * tmpFirstDirectory = staticDirectory + key.MSBs;
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        uint64_t * second_level_directory = dynamicBlock + tmpFirstDirectory->offset;
        auto * segment = segmentBlock + second_level_directory[Util::getMidMSBs(key.midMSBs,tmpFirstDirectory->globalDepth)];
        int old_slot = segment->search_item_with_fingerprint(key.key, position, value.fingerPrint, dataSet);
        if (old_slot >= 0) {
            return segment->modify(old_slot, key);
        }else{
            return false;
        }
    }

    void doubleDirectory(staticDirectoryEntry * tmpFirstDirectory){
        int counter = 0;
        tmpFirstDirectory->globalDepth++ ;
        auto newSegmentPointer = dynamicDirectory->allocPos(tmpFirstDirectory->globalDepth);
        for(int i = 0;i < 1 << tmpFirstDirectory->globalDepth;i += 2,counter++){
            dynamicBlock[newSegmentPointer + i] = dynamicBlock[tmpFirstDirectory->offset + counter];
            dynamicBlock[newSegmentPointer + i + 1] = dynamicBlock[tmpFirstDirectory->offset + counter];
        }
        tmpFirstDirectory->offset = newSegmentPointer;
    }
    void bucketSplit(NVMBucket* preSegment,uint64_t MSBs,staticDirectoryEntry * tmpFirstDirectory, char dataSet[][MAX_LENGTH + 1]){
        uint64_t offset = tmpFirstDirectory->globalDepth - preSegment->localDepth;
        MSBs = (MSBs >> offset) << offset;
        bool extendSet = false;
        if(preSegment->localDepth == tmpFirstDirectory->globalDepth){
            // double directory
            MSBs <<= 1;
            extendSet = true;
        }
        // create segment and transfer data
        uint64_t newSegment = segmentManager->get();
        // reset directory
        preSegment->localDepth++;
        (segmentBlock + newSegment)->localDepth = preSegment->localDepth;
        // strategy (delta, inplace, data-aware)
//        preSegment->copy_mid(segmentBlock + newSegment);
//        preSegment->merge_and_copy_mid(segmentBlock + newSegment,preSegment->localDepth);
        preSegment->copy_mid_strategy(segmentBlock + newSegment,tmpFirstDirectory, dataSet);
#ifdef eADR
        clwb((char*)preSegment,sizeof(NVMBucket));
        clwb((char*)(segmentBlock + newSegment),sizeof(NVMBucket));
#endif
        if(extendSet){
            int counter = 0;
            tmpFirstDirectory->globalDepth++ ;
            auto newSegmentPointer = dynamicDirectory->allocPos(tmpFirstDirectory->globalDepth);
            for(int i = 0;i < 1 << tmpFirstDirectory->globalDepth;i += 2,counter++){
                dynamicBlock[newSegmentPointer + i] = dynamicBlock[tmpFirstDirectory->offset + counter];
                dynamicBlock[newSegmentPointer + i + 1] = dynamicBlock[tmpFirstDirectory->offset + counter];
            }
            for(uint64_t i = MSBs + (1 << (tmpFirstDirectory->globalDepth - preSegment->localDepth))
                    ;i < MSBs + (1 << (tmpFirstDirectory->globalDepth - preSegment->localDepth + 1));i++){
                dynamicBlock[newSegmentPointer + i] = newSegment;
            }
            tmpFirstDirectory->offset = newSegmentPointer;
        }else{
            for(uint64_t i = MSBs + (1 << (tmpFirstDirectory->globalDepth - preSegment->localDepth))
                    ;i < MSBs + (1 << (tmpFirstDirectory->globalDepth - preSegment->localDepth + 1));i++){
                dynamicBlock[tmpFirstDirectory->offset + i] = newSegment;
            }
        }
    }
    bool search(KeyPointer key,ValuePointer value,uint64_t *dataSet){
        staticDirectoryEntry * tmpFirstDirectory = staticDirectory + key.MSBs;
        uint64_t position = key.LSBs * BUCKET_CAPACITY + STORE;
        uint64_t * second_level_directory = dynamicBlock + tmpFirstDirectory->offset;
        auto * segment = segmentBlock + second_level_directory[Util::getMidMSBs(key.midMSBs,tmpFirstDirectory->globalDepth)];
        int old_slot = segment->search_item_with_fingerprint(key.key, position, value.fingerPrint, dataSet);
        if (old_slot >= 0) {
            return true;
        }
        return false;
    }

    void savePointersToMetaData(const string& filePath) {
        staticDirectory = static_cast<staticDirectoryEntry *>(Util::staticAllocatePMSpace(filePath + "NVMDirectory",sizeof(staticDirectoryEntry) * (1 << NVM_DIRECTORY_DEPTH)));
        segmentManager = static_cast<NVMBlockManager *>(Util::staticAllocatePMSpace(filePath + "blockManager",sizeof(NVMBlockManager)));
        dynamicDirectory = static_cast<DynamicDirectory *>(Util::staticAllocatePMSpace(filePath + "dynamicDirectory",sizeof(DynamicDirectory)));
        segmentManager->start = 1;
        std::cout << "save metaData done" << std::endl;
    }

    void loadPointersFromMetaData(const string& filePath) {
        nsTimer recoveryTime;
        recoveryTime.start();
        staticDirectory = static_cast<staticDirectoryEntry *>(Util::staticRecoveryPMSpace(filePath + "NVMDirectory",sizeof(staticDirectoryEntry) * (1 << NVM_DIRECTORY_DEPTH)));
        segmentManager = static_cast<NVMBlockManager *>(Util::staticRecoveryPMSpace(filePath + "blockManager",sizeof(NVMBlockManager)));
        dynamicDirectory = static_cast<DynamicDirectory *>(Util::staticRecoveryPMSpace(filePath + "dynamicDirectory",sizeof(DynamicDirectory)));
        recoveryTime.end();
        std::cout << "recovery metaData done, time:" << recoveryTime.duration() << std::endl;
    }
};

#include <list>
bool consumeFlag = true;
class PersistentProcessor{
public:
    char (*dataSet)[MAX_LENGTH + 1];
    DRAMQueue * dramQueue;
    NVMManager * nvmManager;
    bool * waitForTrans;
    uint32_t listLength = 0;
    uint8_t persistQueueEnd = 0;
    uint8_t persistQueueStart = 0;
    uint32_t persistNode[256] = {0};
    std::list<long long> timer;
    PersistentProcessor(char dataSet[][MAX_LENGTH + 1], DRAMQueue *dramQueue, NVMManager *nvmManager, bool * waitForTrans) : dataSet(dataSet),
                                                                                                                dramQueue(dramQueue),
                                                                                                                nvmManager(nvmManager),
                                                                                                                waitForTrans(waitForTrans)
    {}
    bool getPersistent(){
        while(consumeFlag || listLength){
            if(listLength){
                if(__sync_bool_compare_and_swap(&persistQueueStart, uint8_t (persistQueueEnd - listLength), uint8_t (persistQueueEnd - listLength + 1))){
                    nsTimer tmp;
                    tmp.start();
                    uint32_t MSBs = persistNode[uint8_t (persistQueueStart - 1)];
                    __sync_fetch_and_sub(&listLength, 1);
                    if(__sync_bool_compare_and_swap(&waitForTrans[MSBs], false, true)) {
                        uint8_t number = dramQueue->insertEnd[MSBs];
                        nvmManager->processRange(dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY), dataSet,
                                                 dramQueue->persistStart[MSBs], number, MSBs,
                                                 dramQueue->type + (MSBs * DRAM_QUEUE_CAPACITY));
                        dramQueue->persistStart[MSBs] = number;
                        __sync_bool_compare_and_swap(&waitForTrans[MSBs], true, false);
                    }
                    tmp.end();
                    timer.emplace_back(tmp.duration());
                }
            }
        }
    }

    bool setPersistent(uint32_t queueNumber){
        if(listLength > 8){
            return true;
        }
        uint8_t number = __sync_fetch_and_add(&persistQueueEnd, 1);
        persistNode[number] = queueNumber;
        __sync_fetch_and_add(&listLength, 1);
        return false;
    }

};
class DRAM_Hash_metadata{
public:
    uint8_t fingerprint[(1 << NVM_DIRECTORY_DEPTH) * (DRAM_QUEUE_CAPACITY + STORE)] = {0};
    Entry entry[(1 << NVM_DIRECTORY_DEPTH) * STORE] = {0};
};
class DRAMManager{
public:
    string filePath;
    bool waitForTrans[1 << NVM_DIRECTORY_DEPTH];
    bool SnapShot[1 << NVM_DIRECTORY_DEPTH];
//    bool * status;
    DRAMQueue* dramQueue;
    NVMManager * nvmManager;
    KeyPointer * keyPointerDataSet;
    ValuePointer * valuePointerDataSet;
    uint8_t threshold[1 << NVM_DIRECTORY_DEPTH];
    PersistentProcessor * persistentProcessor;
    DRAM_Hash_metadata tmpMetaData;
    char (*dataSet)[MAX_LENGTH + 1];
    explicit DRAMManager(std::string _filePath, int number) {
        filePath = std::move(_filePath);
        dramQueue = new DRAMQueue();
        nvmManager = new NVMManager();
        nvmManager->init(filePath);

        std::fill(threshold, threshold + (1 << NVM_DIRECTORY_DEPTH), BASE_THRESHOLD);
    }

    void init(char dataSetName[][MAX_LENGTH + 1],KeyPointer * keyPointer,ValuePointer * valuePointer,bool * statusName){
//        status = statusName;
        dataSet = dataSetName;
        persistentProcessor = new PersistentProcessor(dataSetName,dramQueue,nvmManager,waitForTrans);
        keyPointerDataSet = keyPointer;
        valuePointerDataSet = valuePointer;
    }
    void initVariableKey(char dataSetName[][MAX_LENGTH + 1],KeyPointer * keyPointer,ValuePointer * valuePointer){
        dataSet = dataSetName;
        keyPointerDataSet = keyPointer;
        valuePointerDataSet = valuePointer;
    }
    bool remove(KeyPointer key, ValuePointer value, bool requestType){
        uint64_t MSBs = key.MSBs;
        uint8_t number = __sync_fetch_and_add(&dramQueue->insertEnd[MSBs],1);
        dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].key = key;
        dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].value = value;
        dramQueue->type[(MSBs * DRAM_QUEUE_CAPACITY) + number] = requestType;
#ifdef MERGE
        if(number == uint8_t (dramQueue->persistStart[MSBs] + threshold[MSBs])){
            if(__sync_bool_compare_and_swap(&waitForTrans[(MSBs & MASK)], false, true)){
            for(uint64_t persistMSBs = (MSBs & MASK); persistMSBs < (MSBs & MASK) + (1 << PERSIST_DEPTH); persistMSBs++){
                    number = __sync_fetch_and_add(&dramQueue->insertEnd[persistMSBs],1);
                    nvmManager->processRange(dramQueue->entry + (persistMSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[persistMSBs], number, persistMSBs, dramQueue->type + (persistMSBs * DRAM_QUEUE_CAPACITY));
                    dramQueue->persistStart[persistMSBs] = number;
                }
            }
            __sync_bool_compare_and_swap(&waitForTrans[(MSBs & MASK)], true, false);
        }
#else
        if(number == uint8_t (dramQueue->persistStart[MSBs] + (1 << 7) + (1 << 6))){
            if(__sync_bool_compare_and_swap(&waitForTrans[MSBs], false, true)){
                nvmManager->removeRange(dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[MSBs], number, MSBs, dramQueue->type + (MSBs * DRAM_QUEUE_CAPACITY));
                dramQueue->persistStart[MSBs] = number;
                __sync_bool_compare_and_swap(&waitForTrans[MSBs], true, false);
            }
        }
#endif
        return true;
    }

    bool processRequest(KeyPointer key, ValuePointer value, bool requestType){
#ifdef SNAPSHOT
        uint64_t MSBs = key.MSBs;
        if(SnapShot[MSBs]) {
            uint8_t number = __sync_fetch_and_add(&dramQueue->insertEnd[MSBs],1);
            dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].key = key;
            dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].value = value;
            dramQueue->type[(MSBs * DRAM_QUEUE_CAPACITY) + number] = requestType;
#ifdef MERGE
        if(number == uint8_t (dramQueue->persistStart[MSBs] + threshold[MSBs])){
            if(__sync_bool_compare_and_swap(&waitForTrans[(MSBs & MASK)], false, true)){
            for(uint64_t persistMSBs = (MSBs & MASK); persistMSBs < (MSBs & MASK) + (1 << PERSIST_DEPTH); persistMSBs++){
                    number = __sync_fetch_and_add(&dramQueue->insertEnd[persistMSBs],1);
                    nvmManager->processRange(dramQueue->entry + (persistMSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[persistMSBs], number, persistMSBs, dramQueue->type + (persistMSBs * DRAM_QUEUE_CAPACITY));
                    dramQueue->persistStart[persistMSBs] = number;
                }
            }
            __sync_bool_compare_and_swap(&waitForTrans[(MSBs & MASK)], true, false);
        }
#else
            if(number == uint8_t (dramQueue->persistStart[MSBs] + (1 << 7) + (1 << 6))){
                if(__sync_bool_compare_and_swap(&waitForTrans[MSBs], false, true)){
                    nvmManager->processRange(dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[MSBs], number, MSBs, dramQueue->type + (MSBs * DRAM_QUEUE_CAPACITY));
                    dramQueue->persistStart[MSBs] = number;
                    __sync_bool_compare_and_swap(&waitForTrans[MSBs], true, false);
                }
            }
#endif
            return true;
        }
        else {
            if(requestType) {
                // insert
                uint64_t position = MSBs * DRAM_QUEUE_CAPACITY + key.LSBs * DISPARITY;
                uint64_t meta_position = MSBs * STORE + position + STORE;
                // bucket
                for (int i = 0; i < DISPARITY; ++i)
                {
                    if(!(tmpMetaData.fingerprint[meta_position + i] & 1)) {
                        tmpMetaData.fingerprint[meta_position + i] = value.fingerPrint;
                        dramQueue->entry[position + i].key = key;
                        dramQueue->entry[position + i].value = value;
                        return true;
                    }
                    if (tmpMetaData.fingerprint[meta_position + i] == value.fingerPrint)
                    {
                        if (dramQueue->entry[position + i].key.key == key.key)
                        {
                            dramQueue->entry[position + i].value = value;
                            return true;
                        }
                    }
                }
                position = MSBs * STORE;
                meta_position = MSBs * SEGMENT_DATA_NUMBER;
                // stash
                for (int i = 0; i < STORE; ++i) {
                    if(!(tmpMetaData.fingerprint[meta_position + i] & 1)) {
                        tmpMetaData.fingerprint[meta_position + i] = value.fingerPrint;
                        tmpMetaData.entry[position + i].key = key;
                        tmpMetaData.entry[position + i].value = value;
                        return true;
                    }
                    if (tmpMetaData.fingerprint[meta_position + i] == value.fingerPrint)
                    {
                        if (tmpMetaData.entry[position + i].key.key == key.key)
                        {
                            dramQueue->entry[position + i].value = value;
                            return true;
                        }
                    }
                }
                // persistent data
                if(__sync_bool_compare_and_swap(&SnapShot[MSBs], false, true)) {
                    if(__sync_bool_compare_and_swap(&waitForTrans[MSBs], false, true)){
                        nvmManager->directorySnapshotProcess(MSBs, tmpMetaData.fingerprint + (MSBs * SEGMENT_DATA_NUMBER), tmpMetaData.entry + (MSBs * STORE), dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY));
                        __sync_bool_compare_and_swap(&waitForTrans[MSBs], true, false);
                    }
                }else {
                    processRequest(key, value, requestType);
                }
            }else {
                // search
                uint64_t position = MSBs * DRAM_QUEUE_CAPACITY + key.LSBs * DISPARITY;
                uint64_t meta_position = MSBs * STORE + position + STORE;
#ifdef SIMD
                __m128i fingerprint = _mm_loadu_si128((const __m128i*)(tmpMetaData.fingerprint + meta_position));
                __m128i broadFingerprint = _mm_set1_epi8(value.fingerPrint);
                __m128i cmp = _mm_cmpeq_epi8(fingerprint, broadFingerprint);
                int mask = _mm_movemask_epi8(cmp);

                while (mask) {
                    int i = __builtin_ctz(mask);
                    if (dataSet[dramQueue->entry[position + i].value.ValuePoint] == dataSet[value.ValuePoint])
                        return true;
                    mask &= mask - 1;
                }
#else
                // bucket
                for (int i = 0; i < DISPARITY; ++i)
                {
                    if (tmpMetaData.fingerprint[meta_position + i] == value.fingerPrint)
                    {
                        if (dramQueue->entry[position + i].key.key == key.key)
                        {
                            return true;
                        }
                    }
                }
                position = MSBs * STORE;
                meta_position = MSBs * SEGMENT_DATA_NUMBER;
                // stash
                for (int i = 0; i < STORE; ++i) {
                    if (tmpMetaData.fingerprint[meta_position + i] == value.fingerPrint)
                    {
                        if (tmpMetaData.entry[position + i].key.key == key.key)
                        {
                            return true;
                        }
                    }
                }
                return false;
#endif
            }
        }
#else
        uint64_t MSBs = key.MSBs;
        uint8_t number = __sync_fetch_and_add(&dramQueue->insertEnd[MSBs],1);
        dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].key = key;
        dramQueue->entry[(MSBs * DRAM_QUEUE_CAPACITY) + number].value = value;
        dramQueue->type[(MSBs * DRAM_QUEUE_CAPACITY) + number] = requestType;
#ifdef MERGE
        if(number == uint8_t (dramQueue->persistStart[MSBs] + threshold[MSBs])){
            if(__sync_bool_compare_and_swap(&waitForTrans[(MSBs & MASK)], false, true)){
            for(uint64_t persistMSBs = (MSBs & MASK); persistMSBs < (MSBs & MASK) + (1 << PERSIST_DEPTH); persistMSBs++){
                    number = __sync_fetch_and_add(&dramQueue->insertEnd[persistMSBs],1);
                    nvmManager->processRange(dramQueue->entry + (persistMSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[persistMSBs], number, persistMSBs, dramQueue->type + (persistMSBs * DRAM_QUEUE_CAPACITY));
                    dramQueue->persistStart[persistMSBs] = number;
                }
            }
            __sync_bool_compare_and_swap(&waitForTrans[(MSBs & MASK)], true, false);
        }
#else
        if(number == uint8_t (dramQueue->persistStart[MSBs] + Threshold)){
            if(__sync_bool_compare_and_swap(&waitForTrans[MSBs], false, true)){
                nvmManager->processRange(dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[MSBs], number, MSBs, dramQueue->type + (MSBs * DRAM_QUEUE_CAPACITY));
                dramQueue->persistStart[MSBs] = number;
                __sync_bool_compare_and_swap(&waitForTrans[MSBs], true, false);
            }
        }
#endif
        return true;
#endif
    }
    bool processRemainRequest(int pid, uint64_t thread_number){
        for(uint64_t MSBs = (1 << NVM_DIRECTORY_DEPTH) / thread_number * 2 * pid;MSBs < (1 << NVM_DIRECTORY_DEPTH) / thread_number * 2 * (pid + 1);MSBs++){
            nvmManager->processRange(dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[MSBs], dramQueue->insertEnd[MSBs], MSBs, dramQueue->type + (MSBs * DRAM_QUEUE_CAPACITY));
            dramQueue->persistStart[MSBs] = dramQueue->insertEnd[MSBs];
        }
    }
    bool processRemainRequestN(int pid, uint64_t thread_number){
        for(uint64_t MSBs = (1 << NVM_DIRECTORY_DEPTH) / thread_number * pid;MSBs < (1 << NVM_DIRECTORY_DEPTH) / thread_number * (pid + 1);MSBs++){
            nvmManager->processRange(dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[MSBs], dramQueue->insertEnd[MSBs], MSBs, dramQueue->type + (MSBs * DRAM_QUEUE_CAPACITY));
            dramQueue->persistStart[MSBs] = dramQueue->insertEnd[MSBs];
        }
    }
    bool removeRemainRequest(int pid, uint64_t thread_number){
        for(uint64_t MSBs = (1 << NVM_DIRECTORY_DEPTH) / thread_number * 2 * pid;MSBs < (1 << NVM_DIRECTORY_DEPTH) / thread_number * 2 * (pid + 1);MSBs++){
            nvmManager->removeRange(dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[MSBs], dramQueue->insertEnd[MSBs], MSBs, dramQueue->type + (MSBs * DRAM_QUEUE_CAPACITY));
            dramQueue->persistStart[MSBs] = dramQueue->insertEnd[MSBs];
        }
    }
    bool removeRemainRequestN(int pid, uint64_t thread_number){
        for(uint64_t MSBs = (1 << NVM_DIRECTORY_DEPTH) / thread_number * pid;MSBs < (1 << NVM_DIRECTORY_DEPTH) / thread_number * (pid + 1);MSBs++){
            nvmManager->removeRange(dramQueue->entry + (MSBs * DRAM_QUEUE_CAPACITY), dataSet, dramQueue->persistStart[MSBs], dramQueue->insertEnd[MSBs], MSBs, dramQueue->type + (MSBs * DRAM_QUEUE_CAPACITY));
            dramQueue->persistStart[MSBs] = dramQueue->insertEnd[MSBs];
        }
    }
};
class DRAMManagerNUMA{
public:
    DRAMManager* managerNUMAZero;
    DRAMManager* managerNUMAOne;
#ifndef ISNUMA
    DRAMManagerNUMA(){
        managerNUMAZero = new DRAMManager("/pmem0/blockHash",0);
    }

    void changeDataSet(uint64_t * dataSet){
        managerNUMAZero->dataSet = dataSet;
    }
    void init(uint64_t *dataSetName,KeyPointer * keyPointer,ValuePointer * valuePointer, bool * status){
        managerNUMAZero->init(dataSetName,keyPointer,valuePointer, status);
    }
    void initVariableKey(char dataSetName[][MAX_LENGTH + 1],KeyPointer * keyPointer,ValuePointer * valuePointer){
        managerNUMAZero->initVariableKey(dataSetName,keyPointer,valuePointer);
    }

    bool processRequest(KeyPointer key, ValuePointer value, bool type){
        return managerNUMAOne->processRequest(key, value, type);
    }
    bool processRemainRequest(int pid, uint64_t thread_number){
        managerNUMAZero->processRemainRequest(pid / 2, thread_number);
    }
    bool removeRemainRequest(int pid, uint64_t thread_number){
        managerNUMAZero->removeRemainRequest(pid / 2, thread_number);
    }
    bool remove(KeyPointer key, ValuePointer value){
        return managerNUMAZero->remove(key,value,0);
    }
    bool processRemainRequestSingleThread(int pid){
        managerNUMAZero->processRemainRequestN(pid, 1);
    }
    bool removeRemainRequestSingleThread(int pid){
        managerNUMAZero->removeRemainRequestN(pid, 1);
    }
#else
    DRAMManagerNUMA(){
        managerNUMAZero = new DRAMManager("/pmem0/blockHash",0);
        managerNUMAOne = new DRAMManager("/pmem1/blockHash",1);
    }

    void changeDataSet(char dataSet[][MAX_LENGTH + 1]){
        managerNUMAZero->dataSet = dataSet;
        managerNUMAOne->dataSet = dataSet;
    }
    void init(char dataSet[][MAX_LENGTH + 1],KeyPointer * keyPointer,ValuePointer * valuePointer, bool * status){
        managerNUMAZero->init(dataSet,keyPointer,valuePointer, status);
        managerNUMAOne->init(dataSet,keyPointer,valuePointer, status);
    }
    void initVariableKey(char dataSetName[][MAX_LENGTH + 1],KeyPointer * keyPointer,ValuePointer * valuePointer){
        managerNUMAZero->initVariableKey(dataSetName,keyPointer,valuePointer);
        managerNUMAOne->initVariableKey(dataSetName,keyPointer,valuePointer);
    }

    bool processRequest(KeyPointer key, ValuePointer value, bool type){
        if(key.NUMA){
            return managerNUMAOne->processRequest(key, value, type);
        }else{
            return managerNUMAZero->processRequest(key, value, type);
        }
    }
    bool processRemainRequest(int pid, uint64_t thread_number){
        if(pid % 2){
            managerNUMAOne->processRemainRequest(pid / 2, thread_number);
        }else{
            managerNUMAZero->processRemainRequest(pid / 2, thread_number);
        }
    }
    bool removeRemainRequest(int pid, uint64_t thread_number){
        if(pid % 2){
            managerNUMAOne->removeRemainRequest(pid / 2, thread_number);
        }else{
            managerNUMAZero->removeRemainRequest(pid / 2, thread_number);
        }
    }
    bool remove(KeyPointer key, ValuePointer value){
        if(key.NUMA){
            return managerNUMAOne->remove(key,value,0);
        }else{
            return managerNUMAZero->remove(key,value,0);
        }
    }
    bool processRemainRequestSingleThread(int pid){
        managerNUMAOne->processRemainRequestN(pid, 1);
        managerNUMAZero->processRemainRequestN(pid, 1);
    }
    bool removeRemainRequestSingleThread(int pid){
        managerNUMAOne->removeRemainRequestN(pid, 1);
        managerNUMAZero->removeRemainRequestN(pid, 1);
    }
#endif
};

#endif //PIONEER_H