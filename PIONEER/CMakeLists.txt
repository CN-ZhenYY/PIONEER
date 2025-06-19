cmake_minimum_required(VERSION 3.26)
project(PIONEER)
set(CMAKE_CXX_COMPILER /usr/bin/g++-11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_PREFIX_PATH /usr/lib/gcc/x86_64-linux-gnu/11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")

#set(Boost_NO_SYSTEM_PATHS ON)  # 如果你只想查找指定路径
#set(Boost_INCLUDE_DIR "/usr/include/boost")
#set(Boost_LIBRARY_DIR "/usr/lib/x86_64-linux-gnu$")


find_package(OpenMP REQUIRED)
add_executable(PIONEER
        Util.h
        DataGeneration.h
        PIONEER.cpp
        PIONEER.h
#        PIONEER_var.h
)
find_package(gflags REQUIRED)
target_link_libraries(PIONEER gflags)

add_subdirectory(fastalloc)
add_subdirectory(rng)
find_package(PkgConfig REQUIRED)

pkg_search_module(PMEMOBJ REQUIRED libpmemobj libpmemobj++)
link_directories(/usr/lib/x86_64-linux-gnu/libpmem)

#set(BOOST_ROOT "/usr/include")


find_package(Boost REQUIRED COMPONENTS thread)
message(STATUS "Boost_FOUND: ${Boost_FOUND}")
target_include_directories(PIONEER PUBLIC ${Boost_INCLUDE_DIRS})
#target_link_libraries(PIONEER ${PMEMOBJ_LIBRARIES} pmemobj pmem nvmkv-fastalloc nvmkv-rng -lpthread -luuid ${Boost_LIBRARIES} OpenMP::OpenMP_CXX -lpapi)
target_link_libraries(PIONEER ${PMEMOBJ_LIBRARIES} pmemobj pmem nvmkv-fastalloc nvmkv-rng -lpthread -luuid ${Boost_LIBRARIES} OpenMP::OpenMP_CXX)