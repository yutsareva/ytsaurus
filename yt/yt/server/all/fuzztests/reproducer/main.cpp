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
#include <iostream>
#include <fstream>

NYT::NRpc::IServerPtr server;

std::unique_ptr<NYT::NClusterNode::TClusterNodeProgram> DataNode;

void Init() {
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
        std::cout << "Starting datanode ..." << std::endl;
        DataNode->Run(argc, argv);
    });
    serverThread.detach();
    std::this_thread::sleep_for(std::chrono::seconds(20));
}

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

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " FILE_NAME" << std::endl;
        return -1;
    }

    std::fstream str_input(argv[1], std::ios::in | std::ios::binary);
    if (!str_input) {
        std::cerr << "Failed to open file: " << argv[1] << std::endl;
        return -1;
    }

    NYT::NChunkClient::NProto::TFuzzerInput bin_message;
    if (!bin_message.ParseFromIstream(&str_input)) {
        std::cerr << "Failed to parse protobuf message." << std::endl;
        return -1;
    }
    google::protobuf::TextFormat::Printer printer;
    printer.SetUseUtf8StringEscaping(true);

    TProtoStringType text_message;
    std::cout << "Fuzzer input:\n";
    if (!printer.PrintToString(bin_message, &text_message)) {
        std::cerr << "Failed to convert protobuf message to text." << std::endl;
        return -1;
    }

    std::cout << text_message << std::endl;
    // Initialize your fuzzer or environment here if necessary
    Init();

    switch (bin_message.request_case()) {
        case NYT::NChunkClient::NProto::TFuzzerInput::kStartChunk:
            SendRequest("StartChunk", bin_message.start_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::StartChunk);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kFinishChunk:
            SendRequest("FinishChunk", bin_message.finish_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::FinishChunk);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kCancelChunk:
            SendRequest("CancelChunk", bin_message.cancel_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::CancelChunk);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kPingSession:
            SendRequest("PingSession", bin_message.ping_session(), &NYT::NChunkClient::TDataNodeServiceProxy::PingSession);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kPutBlocks:
            SendRequest("PutBlocks", bin_message.put_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::PutBlocks);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kSendBlocks:
            SendRequest("SendBlocks", bin_message.send_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::SendBlocks);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kFlushBlocks:
            SendRequest("FlushBlocks", bin_message.flush_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::FlushBlocks);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kUpdateP2PBlocks:
            SendRequest("UpdateP2PBlocks", bin_message.update_p2p_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::UpdateP2PBlocks);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kProbeChunkSet:
            SendRequest("ProbeChunkSet", bin_message.probe_chunk_set(), &NYT::NChunkClient::TDataNodeServiceProxy::ProbeChunkSet);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kProbeBlockSet:
            SendRequest("ProbeBlockSet", bin_message.probe_block_set(), &NYT::NChunkClient::TDataNodeServiceProxy::ProbeBlockSet);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetBlockSet:
            SendRequest("GetBlockSet", bin_message.get_block_set(), &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockSet);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetBlockRange:
            SendRequest("GetBlockRange", bin_message.get_block_range(), &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockRange);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetChunkFragmentSet:
            SendRequest("GetChunkFragmentSet", bin_message.get_chunk_fragment_set(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkFragmentSet);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kLookupRows:
            SendRequest("LookupRows", bin_message.lookup_rows(), &NYT::NChunkClient::TDataNodeServiceProxy::LookupRows);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetChunkMeta:
            SendRequest("GetChunkMeta", bin_message.get_chunk_meta(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkMeta);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetChunkSliceDataWeights:
            SendRequest("GetChunkSliceDataWeights", bin_message.get_chunk_slice_data_weights(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSliceDataWeights);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetChunkSlices:
            SendRequest("GetChunkSlices", bin_message.get_chunk_slices(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSlices);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetTableSamples:
            SendRequest("GetTableSamples", bin_message.get_table_samples(), &NYT::NChunkClient::TDataNodeServiceProxy::GetTableSamples);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kGetColumnarStatistics:
            SendRequest("GetColumnarStatistics", bin_message.get_columnar_statistics(), &NYT::NChunkClient::TDataNodeServiceProxy::GetColumnarStatistics);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kDisableChunkLocations:
            SendRequest("DisableChunkLocations", bin_message.disable_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::DisableChunkLocations);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kDestroyChunkLocations:
            SendRequest("DestroyChunkLocations", bin_message.destroy_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::DestroyChunkLocations);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kResurrectChunkLocations:
            SendRequest("ResurrectChunkLocations", bin_message.resurrect_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::ResurrectChunkLocations);
            break;
        case NYT::NChunkClient::NProto::TFuzzerInput::kAnnounceChunkReplicas:
            SendRequest("AnnounceChunkReplicas", bin_message.announce_chunk_replicas(), &NYT::NChunkClient::TDataNodeServiceProxy::AnnounceChunkReplicas);
            break;
        default:
            break;
    }

    return 0;
}
