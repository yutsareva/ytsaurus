rm -r /home/yutsareva/yt/coverage/
export LLVM_PROFILE_FILE="/home/yutsareva/yt/coverage/coverage-%p.profraw"

./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode \
    /tmp/new_inputs_fork_13_04 /tmp/datanode_corpus \
    -artifact_prefix=/tmp/fuzzing_artifacts_13_04/ -fork=10 -ignore_crashes=1 -rss_limit_mb=0 -max_total_time=60

llvm-profdata merge -sparse /home/yutsareva/yt/coverage/coverage-*.profraw -o merged.profdata
llvm-cov show -instr-profile=merged.profdata  ./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode  > coverage.txt

# generate HTML report
# llvm-cov report ./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode -instr-profile=merged.profdata
llvm-cov show ./yt/yt/server/all/fuzztests/datanode/fuzztests-datanode -instr-profile=merged.profdata -format=html -o /home/yutsareva/yt/coverage/coverage_report/

# serve coverage report on localhost:8000
cd /home/yutsareva/yt/coverage/coverage_report/
python3 -m http.server 8000

# enable ssh-forwarding
ssh -L 8000:localhost:8000 yutsareva@158.160.26.112

# open http://localhost:8000/ in your browser
