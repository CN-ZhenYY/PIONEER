# PIONEER

This is the source code of PIONEER.

Our paper, "Reconfiguring Scalable Hashing with Persistent CPU Caches", is submitted to SIGMOD2026.

## Directory description

- datasets: We use YCSB to generate 100% insert workload and the dataset in the github is a 10M key-value items demo to run the code.
- PIONEER: The source code for PIONEER

## Linux build
All experiments are conducted on a two-socket server running Ubuntu 18.04 LTS, equipped with two Intel Xeon Gold 6326 processors (2.9 GHz). To enable the enhanced Asynchronous DRAM Refresh (eADR), the system must utilize 3rd generation Intel Xeon Scalable Processors in conjunction with 2nd generation Intel Optane DC Persistent Memory Modules (DCPMM).

### Prerequisites
- install boost and pmdk 
- install cmake, g++

### Build and run steps
Download this repository and change to the PIONEER folder.
```bash
$ cd cmake-build-debug
$ ./START.sh

