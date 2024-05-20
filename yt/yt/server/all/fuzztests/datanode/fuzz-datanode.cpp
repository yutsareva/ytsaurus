#include <yt/yt/server/node/cluster_node/program.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/server/all/fuzztests/lib/timer.h>

#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/yt/logging/logger.h>

#include <google/protobuf/text_format.h>
#include <sstream>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>
#include <unistd.h>  // for sysconf(_SC_PAGESIZE)
#include <thread>

NYT::NRpc::IServerPtr server;

std::unique_ptr<NYT::NClusterNode::TClusterNodeProgram> DataNode;

static const auto& Logger = NYT::NClusterNode::ClusterNodeLogger;

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
        fuzzing::Timer t;
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

size_t getCurrentRSS() {
    std::ifstream statm("/proc/self/statm");
    size_t size = 0, resident = 0;

    if (statm) {
        // Read values from /proc/self/statm
        statm >> size >> resident;  // We only need 'size' and 'resident'
    } else {
        std::cerr << "Failed to open /proc/self/statm" << std::endl;
        return 0;  // Return 0 if file could not be opened
    }

    statm.close();
    long page_size = sysconf(_SC_PAGESIZE); // Get system page size

    return resident * page_size; // Calculate RSS in bytes
}

void MergeReqs(NYT::NChunkClient::NProto::TDatanodeFuzzerInput& first, const NYT::NChunkClient::NProto::TDatanodeFuzzerInput& second) {
    for (const auto& r : second.requests()) {
        auto& new_req = *first.add_requests();
        new_req = r;
    }
    int excess = first.requests().size() - 10000;
    if (excess > 0) {
        first.mutable_requests()->DeleteSubrange(0, excess);
    }
}

std::string getFileName() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);

    std::stringstream day;
    day << std::put_time(std::localtime(&now_time), "%m-%d");
    std::stringstream time;
    time << std::put_time(std::localtime(&now_time), "%H-%M");

    auto thread_id = std::this_thread::get_id();
    auto dir = "/tmp/fuzz_current_request/" + day.str();
    // std::filesystem::create_directories("/tmp/fuzz_current_request/");
    std::filesystem::create_directories(dir);
    std::stringstream s;
    s << dir << "/" << thread_id << "_" << time.str();
    return s.str();
}

void DumpFuzzerInputToFile(const NYT::NChunkClient::NProto::TDatanodeFuzzerInput& fuzzer_input) {
    static auto req = NYT::NChunkClient::NProto::TDatanodeFuzzerInput();
    MergeReqs(req, fuzzer_input);

    static std::string filename = getFileName();

    std::ofstream file(filename, std::ios::out | std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }

    if (!req.SerializeToOstream(&file)) {
        std::cerr << "Failed to serialize TDatanodeFuzzerInput to file: " << filename << std::endl;
        return;
    }
}


template<typename TRequest, typename TProxyMethod>
void SendRequest(const std::string& methodName, const TRequest& request, TProxyMethod proxyMethod,
        const NProtoBuf::RepeatedPtrField<TBasicString<char>>& attachments) {
    const auto before_rss = getCurrentRSS();
    // if (methodName != "GetChunkFragmentSet") return;
    // YT_LOG_INFO("LOOOG Sending %v, attachments size: %v", methodName, attachments.size());
    // if (methodName == "GetChunkFragmentSet") {
    std::cerr << "LOOOG Sending " << methodName << ", attachments size: " << attachments.size() << std::endl;
    // }
    std::string rspMsg = "";
    size_t retry = 0;
    do {
        if (retry) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        fuzzing::Timer t;
        server = DataNode->WaitRpcServer();
        auto channel = NYT::NRpc::CreateLocalChannel(server);
        NYT::NChunkClient::TDataNodeServiceProxy proxy(channel);

        auto req = (proxy.*proxyMethod)();
        req->CopyFrom(request);
        {
            std::vector<NYT::NChunkClient::TBlock> blocks;
            for (const auto& attachment : attachments) {
                blocks.emplace_back(
                    NYT::TSharedRef::FromString(TString(attachment)));
            }
            req->Attachments().reserve(blocks.size());
            for (const auto& block : blocks) {
                req->Attachments().push_back(block.Data);
            }
        }
        auto rspOrError = NYT::NConcurrency::WaitFor(req->Invoke());
        std::stringstream str;
        const auto after_rss = getCurrentRSS();
        auto time = t.Reset();
        str << methodName << " took " << time << " ms, response: " << rspMsg
                << ", retry=" << retry
                << ", attachments size:" << attachments.size() << ", real attach size: " << req->Attachments().size()
                << ", before rss="<<before_rss << ", after_rss="<<after_rss << ", diff=" << (after_rss - before_rss)*1.0 / (1024 * 1024 * 1024) << "GB";
        if (time > 3000) {
            google::protobuf::TextFormat::Printer printer;
            printer.SetUseUtf8StringEscaping(true);

            TProtoStringType text_message;
            printer.PrintToString(request, &text_message);
            str << ", req=" << text_message;            
        }
        // YT_LOG_INFO("LOOOG %v took %v ms, response: %v", methodName, t.Reset(), rspOrError.GetMessage());
        rspMsg = rspOrError.GetMessage();
        // if (after_rss - before_rss > 1024 * 1024 * 1024) {
        YT_LOG_INFO("LOOOG %v", str.str());
        std::cerr << str.str() << std::endl;
        // }
        // std::cerr << methodName << " took " <<  t.Reset() << " ms, response: " << rspMsg
        //         << ", retry=" << retry
        //         << ", attachments size:" << attachments.size() << ", real attach size: " << req->Attachments().size() << std::endl;
        // std::this_thread::sleep_for(std::chrono::seconds(1));
        ++retry;
    } while (rspMsg == "Master is not connected");
}


DEFINE_BINARY_PROTO_FUZZER(const NYT::NChunkClient::NProto::TDatanodeFuzzerInput& fuzzer_input) {
    static bool initialized = Init();
    assert(initialized);
    YT_LOG_INFO("LOOOG FUZZ req size: %v", fuzzer_input.requests().size());
    DumpFuzzerInputToFile(fuzzer_input);

    for (const auto& request_with_attachments : fuzzer_input.requests()) {
        const auto& request = request_with_attachments.request();
        const auto& attachments = request_with_attachments.attachments();
        switch (request.request_case()) {
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kStartChunk:
                SendRequest("StartChunk", request.start_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::StartChunk, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kFinishChunk:
                SendRequest("FinishChunk", request.finish_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::FinishChunk, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kCancelChunk:
                SendRequest("CancelChunk", request.cancel_chunk(), &NYT::NChunkClient::TDataNodeServiceProxy::CancelChunk, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kPingSession:
                SendRequest("PingSession", request.ping_session(), &NYT::NChunkClient::TDataNodeServiceProxy::PingSession, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kPutBlocks:
                SendRequest("PutBlocks", request.put_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::PutBlocks, attachments);
                break;
            // case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kSendBlocks:
            //     SendRequest("SendBlocks", request.send_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::SendBlocks, attachments);
            //     break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kFlushBlocks:
                SendRequest("FlushBlocks", request.flush_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::FlushBlocks, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kUpdateP2PBlocks:
                SendRequest("UpdateP2PBlocks", request.update_p2p_blocks(), &NYT::NChunkClient::TDataNodeServiceProxy::UpdateP2PBlocks, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kProbeChunkSet:
                SendRequest("ProbeChunkSet", request.probe_chunk_set(), &NYT::NChunkClient::TDataNodeServiceProxy::ProbeChunkSet, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kProbeBlockSet:
                SendRequest("ProbeBlockSet", request.probe_block_set(), &NYT::NChunkClient::TDataNodeServiceProxy::ProbeBlockSet, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetBlockSet:
                SendRequest("GetBlockSet", request.get_block_set(), &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockSet, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetBlockRange:
                SendRequest("GetBlockRange", request.get_block_range(), &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockRange, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetChunkFragmentSet:
                SendRequest("GetChunkFragmentSet", request.get_chunk_fragment_set(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkFragmentSet, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kLookupRows:
                SendRequest("LookupRows", request.lookup_rows(), &NYT::NChunkClient::TDataNodeServiceProxy::LookupRows, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetChunkMeta:
                SendRequest("GetChunkMeta", request.get_chunk_meta(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkMeta, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetChunkSliceDataWeights:
                SendRequest("GetChunkSliceDataWeights", request.get_chunk_slice_data_weights(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSliceDataWeights, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetChunkSlices:
                SendRequest("GetChunkSlices", request.get_chunk_slices(), &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSlices, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetTableSamples:
                SendRequest("GetTableSamples", request.get_table_samples(), &NYT::NChunkClient::TDataNodeServiceProxy::GetTableSamples, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetColumnarStatistics:
                SendRequest("GetColumnarStatistics", request.get_columnar_statistics(), &NYT::NChunkClient::TDataNodeServiceProxy::GetColumnarStatistics, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kDisableChunkLocations:
                SendRequest("DisableChunkLocations", request.disable_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::DisableChunkLocations, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kDestroyChunkLocations:
                SendRequest("DestroyChunkLocations", request.destroy_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::DestroyChunkLocations, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kResurrectChunkLocations:
                SendRequest("ResurrectChunkLocations", request.resurrect_chunk_locations(), &NYT::NChunkClient::TDataNodeServiceProxy::ResurrectChunkLocations, attachments);
                break;
            case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kAnnounceChunkReplicas:
                SendRequest("AnnounceChunkReplicas", request.announce_chunk_replicas(), &NYT::NChunkClient::TDataNodeServiceProxy::AnnounceChunkReplicas, attachments);
                break;
            default:
                break;
        }
    }
}
