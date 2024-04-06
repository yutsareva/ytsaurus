#include <yt/yt/server/node/cluster_node/program.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

#include <library/cpp/resource/resource.h>

#include <google/protobuf/text_format.h>
#include <sstream>
#include <string>

NYT::NRpc::IServerPtr server;

std::unique_ptr<NYT::NClusterNode::TClusterNodeProgram> DataNode;

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
    // std::cerr << "req=" << request.DebugString() << std::endl;
    server = DataNode->WaitRpcServer();
    auto channel = NYT::NRpc::CreateLocalChannel(server);
    NYT::NChunkClient::TDataNodeServiceProxy proxy(channel);

    auto req = (proxy.*proxyMethod)();
    req->CopyFrom(request);

    auto rspOrError = NYT::NConcurrency::WaitFor(req->Invoke());
    std::cerr << methodName << " response message: " << rspOrError.GetMessage() << std::endl << std::endl;
}

DEFINE_BINARY_PROTO_FUZZER(const NYT::NChunkClient::NProto::TFuzzerInput& fuzzerInput) {
    static bool initialized = Init();
    assert(initialized);
    switch (fuzzerInput.request_case()) {
        case NYT::NChunkClient::NProto::TFuzzerInput::kStartChunk:
            SendRequest("StartChunk", fuzzerInput.start_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::StartChunk);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kFinishChunk:
            SendRequest("FinishChunk", fuzzerInput.finish_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::FinishChunk);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kCancelChunk:
            SendRequest("CancelChunk", fuzzerInput.cancel_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::CancelChunk);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kPingSession:
            SendRequest("PingSession", fuzzerInput.ping_session(), &NYT::NChunkClient::TDataNodeServiceProxy::PingSession);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kPutBlocks:
            SendRequest("PutBlocks", fuzzerInput.put_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::PutBlocks);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kSendBlocks:
            SendRequest("SendBlocks", fuzzerInput.send_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::SendBlocks);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kFlushBlocks:
            SendRequest("FlushBlocks", fuzzerInput.flush_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::FlushBlocks);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kUpdateP2PBlocks:
            SendRequest("UpdateP2PBlocks", fuzzerInput.update_p2p_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::UpdateP2PBlocks);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kProbeChunkSet:
            SendRequest("ProbeChunkSet", fuzzerInput.probe_chunk_set(), &NYT::NChunkClient::TDataNodeServiceProxy::ProbeChunkSet);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kProbeBlockSet:
            SendRequest("ProbeBlockSet", fuzzerInput.probe_block_set(), &NYT::NChunkClient::TDataNodeServiceProxy::ProbeBlockSet);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetBlockSet:
            SendRequest("GetBlockSet", fuzzerInput.get_block_set(), &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockSet);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetBlockRange:
            SendRequest("GetBlockRange", fuzzerInput.get_block_range(), &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockRange);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetChunkFragmentSet:
            SendRequest("GetChunkFragmentSet", fuzzerInput.get_chunk_fragment_set(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkFragmentSet);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kLookupRows:
            SendRequest("LookupRows", fuzzerInput.lookup_rows(), &NYT::NChunkClient::TDataNodeServiceProxy::LookupRows);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetChunkMeta:
            SendRequest("GetChunkMeta", fuzzerInput.get_chunk_meta(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkMeta);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetChunkSliceDataWeights:
            SendRequest("GetChunkSliceDataWeights", fuzzerInput.get_chunk_slice_data_weights(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSliceDataWeights);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetChunkSlices:
            SendRequest("GetChunkSlices", fuzzerInput.get_chunk_slices(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSlices);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetTableSamples:
            SendRequest("GetTableSamples", fuzzerInput.get_table_samples(), &NYT::NChunkClient::TDataNodeServiceProxy::GetTableSamples);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetColumnarStatistics:
            SendRequest("GetColumnarStatistics", fuzzerInput.get_columnar_statistics(), &NYT::NChunkClient::TDataNodeServiceProxy::GetColumnarStatistics);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kDisableChunkLocations:
            SendRequest("DisableChunkLocations", fuzzerInput.disable_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::DisableChunkLocations);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kDestroyChunkLocations:
            SendRequest("DestroyChunkLocations", fuzzerInput.destroy_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::DestroyChunkLocations);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kResurrectChunkLocations:
            SendRequest("ResurrectChunkLocations", fuzzerInput.resurrect_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::ResurrectChunkLocations);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kAnnounceChunkReplicas:
            SendRequest("AnnounceChunkReplicas", fuzzerInput.announce_chunk_replicas(), &NYT::NChunkClient::TDataNodeServiceProxy::AnnounceChunkReplicas);
            break;
        default:
            break;
    }
}
