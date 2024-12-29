#include "fastalloc.h"
#include <string.h>
fastalloc *myallocator;

fastalloc::fastalloc() {}

void fastalloc::init() {
    dram[dram_cnt] = new char[ALLOC_SIZE];
    dram_curr = dram[dram_cnt];
    dram_left = ALLOC_SIZE;
    dram_cnt++;

#ifdef __linux__
    if (onPM) {
        string nvm_filename = filePath + to_string(nvm_cnt);
        int nvm_fd = open(nvm_filename.c_str(), O_CREAT | O_RDWR, 0644);
        if (posix_fallocate(nvm_fd, 0, ALLOC_SIZE) < 0)
            puts("fallocate fail\n");
        nvm[nvm_cnt] = (char *) mmap(nullptr, ALLOC_SIZE, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE, nvm_fd, 0);
    } else {
        nvm[nvm_cnt] = new char[ALLOC_SIZE];
    }
#else
    nvm[nvm_cnt] = new char[ALLOC_SIZE];
#endif
    nvm_curr = nvm[nvm_cnt];
    nvm_left = ALLOC_SIZE;
    nvm_cnt++;
}
void concurrency_fastalloc::recover(bool _onPM,string _filePath){
    onPM = _onPM;
    std::cout << filePath << std::endl;
    filePath = _filePath;

    dram[dram_cnt] = new char[ALLOC_SIZE];
    dram_curr = dram[dram_cnt];
    dram_left = ALLOC_SIZE;
    dram_cnt++;


#ifdef __linux__
    if (onPM) {
        string nvm_filename = filePath + to_string(nvm_cnt);
        int nvm_fd = open(nvm_filename.c_str(), O_CREAT | O_RDWR, 0644);
        if (posix_fallocate(nvm_fd, 0, ALLOC_SIZE) < 0)
            puts("fallocate fail\n");
        nvm_cnt = 0;
        nvm[nvm_cnt] = (char *) mmap(NULL, ALLOC_SIZE, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE, nvm_fd, 0);
    } else {
        nvm[nvm_cnt] = new char[ALLOC_SIZE];
    }
#else
    nvm[nvm_cnt] = new char[CONCURRENCY_ALLOC_SIZE];
#endif
    nvm_curr = nvm[nvm_cnt];
    nvm_left = ALLOC_SIZE;
    nvm_cnt++;
}
void concurrency_fastalloc::init(bool _onPM, string _filePath) {
    onPM = _onPM;
    std::cout << filePath << std::endl;
    filePath = _filePath;

    dram[dram_cnt] = new char[ALLOC_SIZE];
    dram_curr = dram[dram_cnt];
    dram_left = ALLOC_SIZE;
    dram_cnt++;

#ifdef __linux__
    if (onPM) {
        string nvm_filename = filePath + to_string(nvm_cnt);
        int nvm_fd = open(nvm_filename.c_str(), O_CREAT | O_RDWR, 0644);
        if (posix_fallocate(nvm_fd, 0, ALLOC_SIZE) < 0)
            puts("fallocate fail\n");
        nvm[nvm_cnt] = (char *) mmap(NULL, ALLOC_SIZE, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE, nvm_fd, 0);
    } else {
        nvm[nvm_cnt] = new char[ALLOC_SIZE];
    }
#else
    nvm[nvm_cnt] = new char[CONCURRENCY_ALLOC_SIZE];
#endif
    nvm_curr = nvm[nvm_cnt];
    nvm_left = ALLOC_SIZE;
    nvm_cnt++;
}

void *fastalloc::alloc(uint64_t size, bool _on_nvm) {
    size = size / 256 * 256 + (!!(size % 256)) * 256;
    if (_on_nvm) {
        if (unlikely(size > nvm_left)) {
#ifdef __linux__
            if (onPM) {
                string nvm_filename = filePath + to_string(nvm_cnt);
                int nvm_fd = open(nvm_filename.c_str(), O_CREAT | O_RDWR, 0644);
                if (posix_fallocate(nvm_fd, 0, ALLOC_SIZE) < 0)
                    puts("fallocate fail\n");
                nvm[nvm_cnt] = (char *) mmap(NULL, ALLOC_SIZE, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE, nvm_fd, 0);
            } else {
                nvm[nvm_cnt] = new char[ALLOC_SIZE];
            }
#else
            nvm[nvm_cnt] = new char[ALLOC_SIZE];
#endif
            nvm_curr = nvm[nvm_cnt];
            nvm_left = ALLOC_SIZE;
            nvm_cnt++;
        }
        nvm_left -= size;
        void *tmp = (void*)__sync_fetch_and_add(&nvm_curr, reinterpret_cast<char *>(size));
//            void *tmp = nvm_curr;
//            nvm_curr = nvm_curr + size;
        return tmp;
    } else {
        if (unlikely(size > dram_left)) {
            dram[dram_cnt] = new char[ALLOC_SIZE];
            dram_curr = dram[dram_cnt];
            dram_left = ALLOC_SIZE;
            dram_cnt++;
            dram_left -= size;
            void *tmp = dram_curr;
            dram_curr = dram_curr + size;
            return tmp;
        } else {
            dram_left -= size;
            void *tmp = dram_curr;
            dram_curr = dram_curr + size;
            return tmp;
        }
    }
}
void fastalloc::getCur(uint64_t* left,uint64_t* cnt){
    *left = nvm_left;
    *cnt = nvm_cnt;
}
void fastalloc::free() {
    if (dram != NULL) {
        dram_left = 0;
        for (int i = 0; i < dram_cnt; ++i) {
            delete[]dram[i];
        }
        dram_curr = NULL;
    }
}

void concurrency_fastalloc::init_fast_allocator(bool isMultiThread, bool _onPM, string filePath) {
    if (isMultiThread) {
        init(_onPM, filePath);
    } else {
        myallocator = new fastalloc;
        myallocator->init();
    }
}

void *fast_alloc(uint64_t size, bool _on_nvm) {
    return myallocator->alloc(size, _on_nvm);
}

void *concurrency_fastalloc::concurrency_fast_alloc(uint64_t size, bool _on_nvm) {
    return alloc(size, _on_nvm);
}

void fast_free(concurrency_fastalloc *concurrency_myallocator) {
    if (myallocator != NULL) {
        myallocator->free();
        delete myallocator;
    }

    if (concurrency_myallocator != NULL) {
        concurrency_myallocator->free();
        delete concurrency_myallocator;
    }
}
void getCurr(concurrency_fastalloc *concurrency_myallocator,uint64_t* left,uint64_t* cnt){
    concurrency_myallocator->getCur(left,cnt);
}
