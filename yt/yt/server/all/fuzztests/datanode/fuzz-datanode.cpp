#include <yt/yt/server/node/cluster_node/program.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
// #include <yt/yt/server/master/cell_master/program.h>

#include <yt/yt/core/rpc/local_channel.h>

#include <yt/yt/library/program/program.h>

#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

#include <library/cpp/resource/resource.h>


NYT::NRpc::IServerPtr server;

// void runMasterServer() {
//     int argc = 3;
//     const char* argv[] = {"master",  "--config", "/home/yutsareva/yt/ytsaurus/yt/yt/server/all/fuzztests/datanode/master.yson", nullptr};

//     NYT::NCellMaster::TCellMasterProgram().Run(argc, argv);
// }

std::unique_ptr<NYT::NClusterNode::TClusterNodeProgram> DataNode;

extern "C" int LLVMFuzzerInitialize(int *, const char ***) {
    DataNode = std::make_unique<NYT::NClusterNode::TClusterNodeProgram>();

    const char* envYtRepoPath = std::getenv("YT_REPO_PATH");
    if (!envYtRepoPath || envYtRepoPath[0] == '\0') {
        std::cerr << "Environment variable YT_REPO_PATH is not set or empty." << std::endl;
        exit(1);
    }
    
    std::string ytRepoPath = envYtRepoPath;
    std::thread serverThread([ytRepoPath](){
        const std::string configPath = ytRepoPath + "/yt/yt/server/all/fuzztests/datanode/node.yson";

        int argc = 3;
        const char* argv[] = {"data-node", "--config", configPath.c_str(), nullptr};
        DataNode->Run(argc, argv);
    });
    serverThread.detach();
    return 0;
}

static protobuf_mutator::libfuzzer::PostProcessorRegistration<NYT::NChunkClient::NProto::TSessionId> NonNullSessionId = {
    [](NYT::NChunkClient::NProto::TSessionId* message, unsigned int seed) {
        // std::cerr << "PostProcessorRegistration: " << message->chunk_id().first() << ", " << message->chunk_id().second() << std::endl;
        if (message->chunk_id().first() != 0 && message->chunk_id().second() != 0) {
            return;
        }

        std::mt19937_64 rng(seed);
        std::uniform_int_distribution<uint64_t> dist64(1, UINT64_MAX);

        NYT::NProto::TGuid randomChunkId;
        randomChunkId.set_first(dist64(rng));
        randomChunkId.set_second(dist64(rng));
        message->mutable_chunk_id()->CopyFrom(randomChunkId);
    }};

DEFINE_PROTO_FUZZER(const NYT::NChunkClient::NProto::TReqStartChunk &protoReq)
{
    // std::cerr << "req=" << protoReq.DebugString() << std::endl;
    server = DataNode->WaitRpcServer();

    auto ch = NYT::NRpc::CreateLocalChannel(server);

    NYT::NChunkClient::TDataNodeServiceProxy proxy(ch);
    auto req = proxy.StartChunk();
    req->CopyFrom(protoReq);

    auto rspOrError = NYT::NConcurrency::WaitFor(req->Invoke());
    // std::cerr << "rspOrError IsOK() = " << rspOrError.IsOK() << std::endl;
    std::cerr << "rspOrError GetMessage = " << rspOrError.GetMessage() << std::endl;
}
