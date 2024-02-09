## How to run datanode fuzz test

1. Collect test corpus. See `corpus-collection-txt` branch
2. Build fuzz test: `ninja fuzztests-datanode-multi-req`
3. Run fuzz test with collected test corpus
```bash
build_dir $ ./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode-multi-req /tmp/datanode_dump_txt
```

## Fuzz test description

The Datanode is launched once before any tests are launched. The state of the Datanode is not reset between individual tests
