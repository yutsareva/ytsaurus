get_built_tool_path(
  TOOL_rescompiler_bin
  TOOL_rescompiler_dependency
  tools/rescompiler/bin
  rescompiler
)

add_executable(dist-reproducer)

target_compile_options(dist-reproducer PRIVATE
  -Wdeprecated-this-capture
)
target_link_libraries(dist-reproducer PUBLIC
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
target_link_options(dist-reproducer PRIVATE
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
target_sources(dist-reproducer PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/yt/server/all/fuzztests/dist-reproducer/main.cpp
)
add_test(
  NAME
  dist-reproducer
  COMMAND
  dist-reproducer
)
set_property(
  TEST
  dist-reproducer
  PROPERTY
  LABELS
  SMALL
)
set_property(
  TEST
  dist-reproducer
  PROPERTY
  PROCESSORS
  1
)
target_allocator(dist-reproducer
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(dist-reproducer)
