get_built_tool_path(
  TOOL_rescompiler_bin
  TOOL_rescompiler_dependency
  tools/rescompiler/bin
  rescompiler
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=fuzzer -g")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=fuzzer")

add_executable(fuzztests-datanode-multi-req)

target_compile_options(fuzztests-datanode-multi-req PRIVATE
  -Wdeprecated-this-capture
)
target_link_libraries(fuzztests-datanode-multi-req PUBLIC
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
target_link_options(fuzztests-datanode-multi-req PRIVATE
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
target_sources(fuzztests-datanode-multi-req PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/yt/server/all/fuzztests/datanode/fuzz-datanode.cpp
)
add_test(
  NAME
  fuzztests-datanode-multi-req
  COMMAND
  fuzztests-datanode-multi-req
)
set_property(
  TEST
  fuzztests-datanode-multi-req
  PROPERTY
  LABELS
  SMALL
)
set_property(
  TEST
  fuzztests-datanode-multi-req
  PROPERTY
  PROCESSORS
  1
)
target_allocator(fuzztests-datanode-multi-req
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(fuzztests-datanode-multi-req)
