cmake_minimum_required(VERSION 3.26)
project(PIONEER)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_PREFIX_PATH /usr/lib/gcc/x86_64-linux-gnu/11)
find_package(OpenMP REQUIRED)
add_executable(PIONEER
        Util.h
        DataGeneration.h
        PIONEER.cpp
        PIONEER.h
)

add_subdirectory(fastalloc)
add_subdirectory(rng)
find_package(PkgConfig REQUIRED)

pkg_search_module(PMEMOBJ REQUIRED libpmemobj libpmemobj++)
link_directories(/usr/lib/x86_64-linux-gnu/libpmem)

find_package(Boost REQUIRED COMPONENTS thread)
message(STATUS "Boost_FOUND: ${Boost_FOUND}")
target_include_directories(PIONEER PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(PIONEER ${PMEMOBJ_LIBRARIES} pmemobj pmem nvmkv-fastalloc nvmkv-rng -lpthread -luuid ${Boost_LIBRARIES} OpenMP::OpenMP_CXX -lpapi)