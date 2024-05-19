#include <yt/yt/server/node/cluster_node/program.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/server/master/cell_master/program.h>
#include <yt/yt/server/rpc_proxy/program.h>
#include <yt/yt/server/all/fuzztests/lib/timer.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

// #include <yt/yt/server/lib/rpc_proxy/api_service.h>
#include <yt/yt/client/api/rpc_proxy/api_service_proxy.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/yt/logging/logger.h>

#include <google/protobuf/text_format.h>
#include <sstream>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>
#include <unistd.h>  // for sysconf(_SC_PAGESIZE)

NYT::NRpc::IServerPtr server;

std::unique_ptr<NYT::NClusterNode::TClusterNodeProgram> DataNode;
std::unique_ptr<NYT::NCellMaster::TCellMasterProgram> Master;
std::unique_ptr<NYT::NRpcProxy::TRpcProxyProgram> RpcProxy;

static const auto& Logger = NYT::NRpcProxy::RpcProxyLogger;

// extern "C" int LLVMFuzzerInitialize(int *, const char ***) {
bool Init() {
    DataNode = std::make_unique<NYT::NClusterNode::TClusterNodeProgram>();
    Master = std::make_unique<NYT::NCellMaster::TCellMasterProgram>();
    RpcProxy = std::make_unique<NYT::NRpcProxy::TRpcProxyProgram>();


    const char* envYtRepoPath = std::getenv("YT_REPO_PATH");
    if (!envYtRepoPath || envYtRepoPath[0] == '\0') {
        std::cerr << "Environment variable YT_REPO_PATH is not set or empty." << std::endl;
        exit(1);
    }
    std::string ytRepoPath = envYtRepoPath;
    std::thread masterThread([ytRepoPath](){
        const std::string configPath = ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/master.yson";

        int argc = 3;
        const char* argv[] = {"ytserver-master", "--config", configPath.c_str(), nullptr};
        Master->Run(argc, argv);
    });
    std::thread dataNodeThread([ytRepoPath](){
        const std::string configPath = ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/node.yson";

        int argc = 3;
        const char* argv[] = {"data-node", "--config", configPath.c_str(), nullptr};
        DataNode->Run(argc, argv);
    });
    std::thread rpcProxyThread([ytRepoPath](){
        const std::string configPath = ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/rpc_proxy.yson";

        int argc = 3;
        const char* argv[] = {"proxy", "--config", configPath.c_str(), nullptr};
        RpcProxy->Run(argc, argv);
    });
    masterThread.detach();
    dataNodeThread.detach();
    rpcProxyThread.detach();

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
        statm >> size >> resident;
    } else {
        std::cerr << "Failed to open /proc/self/statm" << std::endl;
        return 0;
    }

    statm.close();
    long page_size = sysconf(_SC_PAGESIZE);
    return resident * page_size;
}

template<typename TRequest, typename TProxyMethod>
void SendRequest(const std::string& methodName, const TRequest& request, TProxyMethod proxyMethod,
        const NProtoBuf::RepeatedPtrField<TBasicString<char>>& attachments) {
    const auto before_rss = getCurrentRSS();
    // if (methodName != "GetChunkFragmentSet") return;
    YT_LOG_INFO("LOOOG Sending %v, attachments size: %v", methodName, attachments.size());
    std::cerr << "LOOOG Sending " << methodName << ", attachments size: " << attachments.size() << std::endl;
    std::string rspMsg = "";
    // size_t retry = 0;
    // // do {
    //     if (retry) {
    //         std::this_thread::sleep_for(std::chrono::seconds(1));
    //     }
        fuzzing::Timer t;
        server = RpcProxy->WaitRpcServer();
        auto channel = NYT::NRpc::CreateLocalChannel(server);
        NYT::NApi::NRpcProxy::TApiServiceProxy proxy(channel);

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
        // YT_LOG_INFO("LOOOG %v took %v ms, response: %v", methodName, t.Reset(), rspOrError.GetMessage());
        rspMsg = rspOrError.GetMessage();
        // std::cerr << methodName << " took " <<  t.Reset() << " ms, response: " << rspMsg
        //         << ", retry=" << retry
        //         << ", attachments size:" << attachments.size() << ", real attach size: " << req->Attachments().size() << std::endl;

        const auto after_rss = getCurrentRSS();
        std::stringstream str;
        str << methodName << " took " <<  t.Reset() << " ms, response: " << rspMsg
            // << ", retry=" << retry
            << ", attachments size:" << attachments.size() << ", real attach size: " << req->Attachments().size()
            << ", before rss="<<before_rss << ", after_rss="<<after_rss << ", diff=" << (after_rss - before_rss)*1.0 / (1024 * 1024 * 1024) << "GB" << std::endl;
        // std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cerr << str.str() << std::endl;
        YT_LOG_INFO("LOOOG %v", str.str());
        // ++retry;
    // } while (rspMsg == "Master is not connected");
}


DEFINE_BINARY_PROTO_FUZZER(const NYT::NApi::NRpcProxy::NProto::TRpcProxyFuzzerInput& fuzzer_input) {
    static bool initialized = Init();
    assert(initialized);
    YT_LOG_INFO("LOOOG FUZZ req size: %v", fuzzer_input.requests().size());

    for (const auto& request_with_attachments : fuzzer_input.requests()) {
        const auto& request = request_with_attachments.request();
        const auto& attachments = request_with_attachments.attachments();
        switch (request.request_case()) {
            case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kCreateNode:
                SendRequest("CreateNode", request.create_node(), &NYT::NApi::NRpcProxy::TApiServiceProxy::CreateNode, attachments);
                break;
            case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kCreateObject:
                SendRequest("CreateObject", request.create_object(), &NYT::NApi::NRpcProxy::TApiServiceProxy::CreateObject, attachments);
                break;
            case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kListNode:
                SendRequest("ListNode", request.list_node(), &NYT::NApi::NRpcProxy::TApiServiceProxy::ListNode, attachments);
                break;
            case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kGetNode:
                SendRequest("GetNode", request.get_node(), &NYT::NApi::NRpcProxy::TApiServiceProxy::GetNode, attachments);
                break;
            case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kReadFile:
                SendRequest("ReadFile", request.read_file(), &NYT::NApi::NRpcProxy::TApiServiceProxy::ReadFile, attachments);
                break;
            case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteFile:
                SendRequest("WriteFile", request.write_file(), &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteFile, attachments);
                break;
            // case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kReadTable:
            //     SendRequest("ReadTable", request.read_table(), &NYT::NApi::NRpcProxy::TApiServiceProxy::ReadTable, attachments);
            //     break;
            // case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteTable:
            //     SendRequest("WriteTable", request.write_table(), &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteTable, attachments);
            //     break;
            // case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteTableMeta:
            //     SendRequest("WriteTableMeta", request.write_table_meta(), &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteTableMeta, attachments);
            //     break;
            default:
                break;
        }
    }
}
// NYT::NApi::NRpcProxy {

// ////////////////////////////////////////////////////////////////////////////////

// class TApiServiceProxy

// NYT.NApi.NRpcProxy.NProto