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

template<typename TRequest, typename TProxyMethod>
void SendRequest(const std::string& methodName, const TRequest& request, TProxyMethod proxyMethod) {
    // std::cerr << "req=" << request.DebugString() << std::endl;
    server = DataNode->WaitRpcServer();
    auto channel = NYT::NRpc::CreateLocalChannel(server);
    NYT::NChunkClient::TDataNodeServiceProxy proxy(channel);

    auto req = (proxy.*proxyMethod)();
    req->CopyFrom(request);

    auto rspOrError = NYT::NConcurrency::WaitFor(req->Invoke());
    std::cerr << methodName << " response message: " << rspOrError.GetMessage() << std::endl;
}


extern "C" size_t LLVMFuzzerCustomMutator(uint8_t *Data, size_t Size, size_t MaxSize, unsigned int Seed) {
    std::string input(reinterpret_cast<const char*>(Data), Size);

    std::stringstream ss(input);
    std::string requestName;
    std::getline(ss, requestName);

    std::unique_ptr<google::protobuf::Message> message;
    if (requestName == "StartChunk") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqStartChunk>();
    } else if (requestName == "FinishChunk") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqFinishChunk>();
    } else if (requestName == "CancelChunk") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqCancelChunk>();
    } else if (requestName == "PingSession") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqPingSession>();
    } else if (requestName == "PutBlocks") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqPutBlocks>();
    } else if (requestName == "SendBlocks") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqSendBlocks>();
    } else if (requestName == "FlushBlocks") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqFlushBlocks>();
    } else if (requestName == "UpdateP2PBlocks") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqUpdateP2PBlocks>();
    } else if (requestName == "ProbeChunkSet") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqProbeChunkSet>();
    } else if (requestName == "ProbeBlockSet") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqProbeBlockSet>();
    } else if (requestName == "GetBlockSet") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqGetBlockSet>();
    } else if (requestName == "GetBlockRange") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqGetBlockRange>();
    } else if (requestName == "GetChunkFragmentSet") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqGetChunkFragmentSet>();
    } else if (requestName == "LookupRows") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqLookupRows>();
    } else if (requestName == "GetChunkMeta") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqGetChunkMeta>();
    } else if (requestName == "GetChunkSliceDataWeights") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqGetChunkSliceDataWeights>();
    } else if (requestName == "GetChunkSlices") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqGetChunkSlices>();
    } else if (requestName == "GetTableSamples") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqGetTableSamples>();
    } else if (requestName == "GetColumnarStatistics") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqGetColumnarStatistics>();
    } else if (requestName == "DisableChunkLocations") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqDisableChunkLocations>();
    } else if (requestName == "DestroyChunkLocations") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqDestroyChunkLocations>();
    } else if (requestName == "ResurrectChunkLocations") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqResurrectChunkLocations>();
    } else if (requestName == "AnnounceChunkReplicas") {
        message = std::make_unique<NYT::NChunkClient::NProto::TReqAnnounceChunkReplicas>();
    }


    auto* protoMessageStartPos = Data + requestName.size() + 1;

    auto res = protobuf_mutator::libfuzzer::CustomProtoMutator(
        /*binary=*/false,
        reinterpret_cast<uint8_t*>(protoMessageStartPos),
        Size - 1 - requestName.size(),
        MaxSize - 1 - requestName.size(),
        Seed, message.get());

    if (res == 0) {
        return res;
    }
    return res + requestName.size() + 1;
}

  extern "C" size_t LLVMFuzzerCustomCrossOver(                                
      const uint8_t* data1, size_t size1, const uint8_t* data2, size_t size2, 
      uint8_t* out, size_t max_out_size, unsigned int seed) {
    // TODO
    return 0;      
  }

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* Data, size_t Size) {
    NYT::NChunkClient::NProto::TFuzzerInput fuzzerInput;
    if (!protobuf_mutator::libfuzzer::LoadProtoInput(false, Data, Size, &fuzzerInput)) {
        return 0;
    }

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
    return 0;
}
