## How to run datanode fuzz test

1. Set `YT_REPO_PATH` env var
2. Collect corpus:

    a. Build ytserver-all. Important: set ENABLE_DUMP_PROTO_MESSAGE cmake flag
    ```
    build_dir $ cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=$YT_REPO_PATH/ytsaurus/clang.toolchain -DENABLE_DUMP_PROTO_MESSAGE=1 $YT_REPO_PATH/ytsaurus 
    build_dir $ ninja ytserver-all
     ```

    b. Run integration tests. Corpus will be written to `/tmp/datanode_dump_bin`
3. Start master process (should not be compiled with fuzzer instrumentation, i.e. ENABLE_DUMP_PROTO_MESSAGE is not set)
```bash
ytserver-master --config $YT_REPO_PATH/yt/yt/server/all/fuzztests/datanode/master.yson
```
4. Build fuzz test. Important: set ENABLE_FUZZER cmake flag
```bash
build_fuzz $ cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=../ytsaurus/clang.toolchain -DENABLE_FUZZER=ON ../ytsaurus
build_fuzz $ ninja fuzztests-datanode
```
5. Run fuzz test
```bash
build_fuzz $ ./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode /tmp/datanode_dump_bin
```

## Fuzz test description

The Datanode is launched once before any tests are launched. The state of the Datanode is not reset between individual tests
