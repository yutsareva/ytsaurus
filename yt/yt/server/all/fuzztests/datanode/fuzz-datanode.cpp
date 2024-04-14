#include <yt/yt/server/node/cluster_node/program.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/yt/logging/logger.h>

#include <google/protobuf/text_format.h>
#include <sstream>
#include <string>
#include <chrono>

NYT::NRpc::IServerPtr server;

std::unique_ptr<NYT::NClusterNode::TClusterNodeProgram> DataNode;

static const auto& Logger = NYT::NClusterNode::ClusterNodeLogger;

class Timer {
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}

    int64_t Reset() {
        auto now = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_).count();
        start_ = now;
        return elapsed;
    }

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start_;
};

// extern "C" int LLVMFuzzerInitialize(int *, const char ***) {
bool Init() {
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
    std::this_thread::sleep_for(std::chrono::seconds(10));
    return true;
}

bool IsValidChunkType(const NYT::NChunkClient::NProto::TSessionId& protoSessionId) {
    auto sessionId = NYT::FromProto<NYT::NChunkClient::TSessionId>(protoSessionId);

    auto chunkType = NYT::NObjectClient::TypeFromId(NYT::NChunkClient::DecodeChunkId(sessionId.ChunkId).Id);
    switch (chunkType) {
        case NYT::NObjectClient::EObjectType::Chunk:
        case NYT::NObjectClient::EObjectType::ErasureChunk:
        case NYT::NObjectClient::EObjectType::JournalChunk:
        case NYT::NObjectClient::EObjectType::ErasureJournalChunk:
            return true;
        default:
            return false;
    }
}

static protobuf_mutator::libfuzzer::PostProcessorRegistration<NYT::NChunkClient::NProto::TSessionId> NonNullSessionId = {
    [](NYT::NChunkClient::NProto::TSessionId* message, unsigned int seed) {
        Timer t;
        // Fixes 'No write location is available'
        message->set_medium_index(0);

        // Fixes 'Invalid session chunk type'
        std::mt19937_64 rng(seed);
        std::uniform_int_distribution<uint64_t> dist64(1, UINT64_MAX);

        bool isValidChunkType = IsValidChunkType(*message);
        while (!isValidChunkType) {
            NYT::NProto::TGuid randomChunkId;
            randomChunkId.set_first(dist64(rng));
            randomChunkId.set_second(dist64(rng));
            message->mutable_chunk_id()->CopyFrom(randomChunkId);

            isValidChunkType = IsValidChunkType(*message);
        };
        YT_LOG_INFO("PostProcessor TSessionId took %v ms", t.Reset());
    }};


static protobuf_mutator::libfuzzer::PostProcessorRegistration<NYT::NChunkClient::NProto::TReqUpdateP2PBlocks> NonNegativeChunkBlockCount = {
    [](NYT::NChunkClient::NProto::TReqUpdateP2PBlocks* message, unsigned int seed) {
        for (int i = 0; i < message->chunk_block_count_size(); ++i) {
            int32_t count = message->chunk_block_count(i);
            if (count < 0) {
                message->set_chunk_block_count(i, 0);
            }
        }
    }};


template<typename TRequest, typename TProxyMethod>
void SendRequest(const std::string& methodName, const TRequest& request, TProxyMethod proxyMethod) {
    Timer t;
    server = DataNode->WaitRpcServer();
    auto channel = NYT::NRpc::CreateLocalChannel(server);
    NYT::NChunkClient::TDataNodeServiceProxy proxy(channel);

    auto req = (proxy.*proxyMethod)();
    req->CopyFrom(request);

    auto rspOrError = NYT::NConcurrency::WaitFor(req->Invoke());
    YT_LOG_INFO("%v took %v ms, response: %v", methodName, t.Reset(), rspOrError.GetMessage());
}

DEFINE_BINARY_PROTO_FUZZER(const NYT::NChunkClient::NProto::TFuzzerInput& fuzzer_input) {
    static bool initialized = Init();
    assert(initialized);

    for (const auto& request : fuzzer_input.requests()) {
        switch (request.request_case()) {
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kStartChunk:
                SendRequest("StartChunk", request.start_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::StartChunk);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kFinishChunk:
                SendRequest("FinishChunk", request.finish_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::FinishChunk);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kCancelChunk:
                SendRequest("CancelChunk", request.cancel_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::CancelChunk);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kPingSession:
                SendRequest("PingSession", request.ping_session(), &NYT::NChunkClient::TDataNodeServiceProxy::PingSession);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kPutBlocks:
                SendRequest("PutBlocks", request.put_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::PutBlocks);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kSendBlocks:
                SendRequest("SendBlocks", request.send_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::SendBlocks);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kFlushBlocks:
                SendRequest("FlushBlocks", request.flush_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::FlushBlocks);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kUpdateP2PBlocks:
                SendRequest("UpdateP2PBlocks", request.update_p2p_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::UpdateP2PBlocks);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kProbeChunkSet:
                SendRequest("ProbeChunkSet", request.probe_chunk_set(), &NYT::NChunkClient::TDataNodeServiceProxy::ProbeChunkSet);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kProbeBlockSet:
                SendRequest("ProbeBlockSet", request.probe_block_set(), &NYT::NChunkClient::TDataNodeServiceProxy::ProbeBlockSet);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kGetBlockSet:
                SendRequest("GetBlockSet", request.get_block_set(), &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockSet);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kGetBlockRange:
                SendRequest("GetBlockRange", request.get_block_range(), &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockRange);
                break;
            // case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kGetChunkFragmentSet:
            //     SendRequest("GetChunkFragmentSet", request.get_chunk_fragment_set(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkFragmentSet);
            //     break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kLookupRows:
                SendRequest("LookupRows", request.lookup_rows(), &NYT::NChunkClient::TDataNodeServiceProxy::LookupRows);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kGetChunkMeta:
                SendRequest("GetChunkMeta", request.get_chunk_meta(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkMeta);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kGetChunkSliceDataWeights:
                SendRequest("GetChunkSliceDataWeights", request.get_chunk_slice_data_weights(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSliceDataWeights);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kGetChunkSlices:
                SendRequest("GetChunkSlices", request.get_chunk_slices(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSlices);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kGetTableSamples:
                SendRequest("GetTableSamples", request.get_table_samples(), &NYT::NChunkClient::TDataNodeServiceProxy::GetTableSamples);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kGetColumnarStatistics:
                SendRequest("GetColumnarStatistics", request.get_columnar_statistics(), &NYT::NChunkClient::TDataNodeServiceProxy::GetColumnarStatistics);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kDisableChunkLocations:
                SendRequest("DisableChunkLocations", request.disable_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::DisableChunkLocations);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kDestroyChunkLocations:
                SendRequest("DestroyChunkLocations", request.destroy_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::DestroyChunkLocations);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kResurrectChunkLocations:
                SendRequest("ResurrectChunkLocations", request.resurrect_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::ResurrectChunkLocations);
                break;
            case NYT::NChunkClient::NProto::TFuzzerSingleRequest::kAnnounceChunkReplicas:
                SendRequest("AnnounceChunkReplicas", request.announce_chunk_replicas(), &NYT::NChunkClient::TDataNodeServiceProxy::AnnounceChunkReplicas);
                break;
            default:
                break;
        }
    }
}
