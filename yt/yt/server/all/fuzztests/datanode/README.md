## How to run datanode fuzz test

1. Set `YT_REPO_PATH` env var
2. Collect corpus:

    a. Build ytserver-all. Important: set `ENABLE_DUMP_PROTO_MESSAGE` CMake flag
    ```
    build_dir $ cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_TOOLCHAIN_FILE=$YT_REPO_PATH/clang.toolchain -DENABLE_DUMP_PROTO_MESSAGE=1 $YT_REPO_PATH 
    build_dir $ ninja ytserver-all
     ```

    b. Run integration tests. Corpus will be written to `/tmp/datanode_dump_bin`
3. Start master process (compile without fuzzer instrumentation; do not set ENABLE_DUMP_PROTO_MESSAGE)
```bash
ytserver-master --config $YT_REPO_PATH/yt/yt/server/all/fuzztests/datanode/master.yson
```
4. Build fuzz test. Important: set `ENABLE_FUZZER` CMake flag
```bash
build_fuzz $ cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_TOOLCHAIN_FILE=../ytsaurus/clang.toolchain -DENABLE_FUZZER=ON ../ytsaurus
build_fuzz $ ninja fuzztests-datanode
```
5. Run fuzz test
```bash
build_fuzz $ ./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode /tmp/new_inputs /tmp/datanode_corpus -artifact_prefix=/tmp/fuzzing_artifacts/ -fork=1 -ignore_crashes=1
```

Crashes are saved in `/tmp/fuzzing_artifacts/`.

Inputs revealing new code paths are saved in `/tmp/new_inputs`.

6. Reproduce found crashes.
- Build `reproducer`
```bash
build_dir $ cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_TOOLCHAIN_FILE=$YT_REPO_PATH/clang.toolchain -DENABLE_DUMP_PROTO_MESSAGE=1 $YT_REPO_PATH 
build_dir $ ninja reproducer
```
- Run data node and replay requests that cause crashes
```bash
build_dir $ ./yt/yt/server/all/fuzztests/reproducer/reproducer /tmp/fuzzing_artifacts/<crash id>
```

7. Print fuzzer input in a human-readable format
```bash
build_dir $ ./yt/yt/server/all/fuzztests/protobuf-reader/protobuf-reader /tmp/fuzzing_artifacts/crash-e23435d3d2e44d9cae108e7e484e45147fc3fe37
```

Print methods of unique requests from sample input files.
```bash
build_dir $ for file in /tmp/fuzzing_artifacts/*; do echo "Processing $file:"; ./yt/yt/server/all/fuzztests/protobuf-reader/protobuf-reader "$file" | head -n 1; done
```

## Fuzz test description

The Datanode is launched once before any tests are launched. The state of the Datanode is not reset between individual tests
