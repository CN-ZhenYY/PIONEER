# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.27

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /home/dell/rfh/cmake-3.27.0-linux-x86_64/bin/cmake

# The command to remove a file.
RM = /home/dell/rfh/cmake-3.27.0-linux-x86_64/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/dell/yzy/YCSB_baseline/PIONEER

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/dell/yzy/YCSB_baseline/PIONEER/build

# Include any dependencies generated for this target.
include CMakeFiles/PIONEER.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/PIONEER.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/PIONEER.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/PIONEER.dir/flags.make

CMakeFiles/PIONEER.dir/PIONEER.cpp.o: CMakeFiles/PIONEER.dir/flags.make
CMakeFiles/PIONEER.dir/PIONEER.cpp.o: /home/dell/yzy/YCSB_baseline/PIONEER/PIONEER.cpp
CMakeFiles/PIONEER.dir/PIONEER.cpp.o: CMakeFiles/PIONEER.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --progress-dir=/home/dell/yzy/YCSB_baseline/PIONEER/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/PIONEER.dir/PIONEER.cpp.o"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/PIONEER.dir/PIONEER.cpp.o -MF CMakeFiles/PIONEER.dir/PIONEER.cpp.o.d -o CMakeFiles/PIONEER.dir/PIONEER.cpp.o -c /home/dell/yzy/YCSB_baseline/PIONEER/PIONEER.cpp

CMakeFiles/PIONEER.dir/PIONEER.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Preprocessing CXX source to CMakeFiles/PIONEER.dir/PIONEER.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/dell/yzy/YCSB_baseline/PIONEER/PIONEER.cpp > CMakeFiles/PIONEER.dir/PIONEER.cpp.i

CMakeFiles/PIONEER.dir/PIONEER.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Compiling CXX source to assembly CMakeFiles/PIONEER.dir/PIONEER.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/dell/yzy/YCSB_baseline/PIONEER/PIONEER.cpp -o CMakeFiles/PIONEER.dir/PIONEER.cpp.s

# Object files for target PIONEER
PIONEER_OBJECTS = \
"CMakeFiles/PIONEER.dir/PIONEER.cpp.o"

# External object files for target PIONEER
PIONEER_EXTERNAL_OBJECTS =

PIONEER: CMakeFiles/PIONEER.dir/PIONEER.cpp.o
PIONEER: CMakeFiles/PIONEER.dir/build.make
PIONEER: /usr/local/anaconda3/lib/libgflags.so.2.2.2
PIONEER: fastalloc/libnvmkv-fastalloc.a
PIONEER: rng/libnvmkv-rng.a
PIONEER: /usr/local/anaconda3/lib/libboost_thread.so.1.73.0
PIONEER: /usr/lib/gcc/x86_64-linux-gnu/11/libgomp.so
PIONEER: /usr/lib/x86_64-linux-gnu/libpthread.so
PIONEER: CMakeFiles/PIONEER.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --bold --progress-dir=/home/dell/yzy/YCSB_baseline/PIONEER/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable PIONEER"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/PIONEER.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/PIONEER.dir/build: PIONEER
.PHONY : CMakeFiles/PIONEER.dir/build

CMakeFiles/PIONEER.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/PIONEER.dir/cmake_clean.cmake
.PHONY : CMakeFiles/PIONEER.dir/clean

CMakeFiles/PIONEER.dir/depend:
	cd /home/dell/yzy/YCSB_baseline/PIONEER/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/dell/yzy/YCSB_baseline/PIONEER /home/dell/yzy/YCSB_baseline/PIONEER /home/dell/yzy/YCSB_baseline/PIONEER/build /home/dell/yzy/YCSB_baseline/PIONEER/build /home/dell/yzy/YCSB_baseline/PIONEER/build/CMakeFiles/PIONEER.dir/DependInfo.cmake "--color=$(COLOR)"
.PHONY : CMakeFiles/PIONEER.dir/depend

