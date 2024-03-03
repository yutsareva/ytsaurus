get_built_tool_path(
  TOOL_rescompiler_bin
  TOOL_rescompiler_dependency
  tools/rescompiler/bin
  rescompiler
)

add_executable(reproducer)

target_compile_options(reproducer PRIVATE
  -Wdeprecated-this-capture
)
target_link_libraries(reproducer PUBLIC
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
target_link_options(reproducer PRIVATE
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
target_sources(reproducer PRIVATE
  ${CMAKE_SOURCE_DIR}/yt/yt/server/all/fuzztests/reproducer/main.cpp
)
add_test(
  NAME
  reproducer
  COMMAND
  reproducer
)
set_property(
  TEST
  reproducer
  PROPERTY
  LABELS
  SMALL
)
set_property(
  TEST
  reproducer
  PROPERTY
  PROCESSORS
  1
)
target_allocator(reproducer
  cpp-malloc-tcmalloc
  libs-tcmalloc-default
)
vcs_info(reproducer)
