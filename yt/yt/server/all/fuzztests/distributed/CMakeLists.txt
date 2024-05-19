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

add_executable(fuzztests-distributed)

target_compile_options(fuzztests-distributed PRIVATE
  -Wdeprecated-this-capturenin
)
target_link_libraries(fuzztests-distributed PUBLIC
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
  yt-server-rpc_proxy
)
target_link_options(fuzztests-distributed PRIVATE
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
target_sources(fuzztests-distributed PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/yt/server/all/fuzztests/distributed/fuzz-distributed.cpp
)
add_test(
  NAME
  fuzztests-distributed
  COMMAND
  fuzztests-distributed
)
set_property(
  TEST
  fuzztests-distributed
  PROPERTY
  LABELS
  SMALL
)
set_property(
  TEST
  fuzztests-distributed
  PROPERTY
  PROCESSORS
  1
)
target_allocator(fuzztests-distributed
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(fuzztests-distributed)
