## How to run datanode fuzz test

0. Set `YT_REPO_PATH` env var
1. Start master process
```bash
ytserver-master --config $YT_REPO_PATH/yt/yt/server/all/fuzztests/datanode/master.yson
```
2. Build fuzz test
```bash
build_dir $ cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=$YT_REPO_PATH/ytsaurus/clang.toolchain $YT_REPO_PATH/ytsaurus
build_dir $ ninja fuzztests-datanode
```
3. Run fuzz test
```bash
build_dir $ ./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode
```

## Fuzz test description

The Datanode is launched once before any tests are launched. The state of the Datanode is not reset between individual tests
