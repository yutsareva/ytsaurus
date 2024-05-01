get_built_tool_path(
  TOOL_rescompiler_bin
  TOOL_rescompiler_dependency
  tools/rescompiler/bin
  rescompiler
)


if(ENABLE_COVERAGE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=fuzzer -fprofile-instr-generate -fcoverage-mapping")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=fuzzer -fprofile-instr-generate -fcoverage-mapping")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=fuzzer")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=fuzzer")
endif()

add_executable(fuzztests-datanode)

target_compile_options(fuzztests-datanode PRIVATE
  -Wdeprecated-this-capturenin
)
target_link_libraries(fuzztests-datanode PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  contrib-libs-protobuf-mutator
  yutil
  library-cpp-cpuid_check
  cpp-testing-gtest
  cpp-testing-gtest_main
  library-cpp-resource
  yt-server-master
  yt-server-node
)
target_link_options(fuzztests-datanode PRIVATE
  -ldl
  -lrt
  -Wl,--no-as-needed
  -fPIC
  -fPIC
  -lpthread
  -lrt
  -ldl
  -lutil
)
target_sources(fuzztests-datanode PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/yt/server/all/fuzztests/datanode/fuzz-datanode.cpp
)
add_test(
  NAME
  fuzztests-datanode
  COMMAND
  fuzztests-datanode
)
set_property(
  TEST
  fuzztests-datanode
  PROPERTY
  LABELS
  SMALL
)
set_property(
  TEST
  fuzztests-datanode
  PROPERTY
  PROCESSORS
  1
)
target_allocator(fuzztests-datanode
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(fuzztests-datanode)
