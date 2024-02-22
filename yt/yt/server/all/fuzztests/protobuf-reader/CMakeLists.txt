get_built_tool_path(
  TOOL_rescompiler_bin
  TOOL_rescompiler_dependency
  tools/rescompiler/bin
  rescompiler
)

add_executable(protobuf-reader)

target_compile_options(protobuf-reader PRIVATE
  -Wdeprecated-this-capture
)
target_link_libraries(protobuf-reader PUBLIC
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
target_link_options(protobuf-reader PRIVATE
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
target_sources(protobuf-reader PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/yt/server/all/fuzztests/protobuf-reader/main.cpp
)
add_test(
  NAME
  protobuf-reader
  COMMAND
  protobuf-reader
)
set_property(
  TEST
  protobuf-reader
  PROPERTY
  LABELS
  SMALL
)
set_property(
  TEST
  protobuf-reader
  PROPERTY
  PROCESSORS
  1
)
target_allocator(protobuf-reader
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(protobuf-reader)
