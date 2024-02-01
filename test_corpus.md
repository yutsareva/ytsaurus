## How to collect test corpus

1. Checkout branch `corpus-collection`
2. Run `bash $YTSAURUS_PATH/yt/python/packages/build_ytsaurus_packages.sh --ytsaurus-source-path $YTSAURUS_PATH --ytsaurus-build-path $BUILD_PATH`
2. Follow `Rebuild docker image` instructions from [yt/docker/local/README.md](yt/docker/local/README.md)
3. Run `./run_local_cluster.sh --yt-skip-pull true`
4. Create some files using UI. StartChunk messages will be dumped to `/tmp/datanode_start_chunk_dump` dir
5. Checkout branch `fuzzing`
6. Run fuzz-test and provide path to test corpus
```bash
build_dir $ ./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode /tmp/datanode_start_chunk_dump
```

