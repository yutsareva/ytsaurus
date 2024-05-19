// #include <yt/yt/server/node/cluster_node/program.h>
// #include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

// #include <yt/yt/server/master/cell_master/program.h>
// #include <yt/yt/server/rpc_proxy/program.h>
// #include <yt/yt/server/all/fuzztests/lib/timer.h>

// #include <yt/yt/core/rpc/local_channel.h>
// #include <yt/yt/library/program/program.h>
// #include <yt/yt/ytlib/chunk_client/session_id.h>

// #include <library/cpp/getopt/small/last_getopt_parse_result.h>
// #include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>
// #include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
// #include <yt/yt/ytlib/chunk_client/block.h>
// #include <yt/yt/ytlib/chunk_client/helpers.h>

// // #include <yt/yt/server/lib/rpc_proxy/api_service.h>
// #include <yt/yt/client/api/rpc_proxy/api_service_proxy.h>

// #include <library/cpp/resource/resource.h>
// #include <library/cpp/yt/logging/logger.h>

// #include <google/protobuf/text_format.h>
// #include <sstream>
// #include <string>
// #include <chrono>
// #include <iostream>
// #include <fstream>
// #include <unistd.h>  // for sysconf(_SC_PAGESIZE)

// NYT::NRpc::IServerPtr server;

// std::unique_ptr<NYT::NClusterNode::TClusterNodeProgram> DataNode;
// std::unique_ptr<NYT::NCellMaster::TCellMasterProgram> Master;
// std::unique_ptr<NYT::NRpcProxy::TRpcProxyProgram> RpcProxy;

// static const auto& Logger = NYT::NRpcProxy::RpcProxyLogger;

// // extern "C" int LLVMFuzzerInitialize(int *, const char ***) {
// bool Init() {
//     DataNode = std::make_unique<NYT::NClusterNode::TClusterNodeProgram>();
//     Master = std::make_unique<NYT::NCellMaster::TCellMasterProgram>();
//     RpcProxy = std::make_unique<NYT::NRpcProxy::TRpcProxyProgram>();


//     const char* envYtRepoPath = std::getenv("YT_REPO_PATH");
//     if (!envYtRepoPath || envYtRepoPath[0] == '\0') {
//         std::cerr << "Environment variable YT_REPO_PATH is not set or empty." << std::endl;
//         exit(1);
//     }
//     std::string ytRepoPath = envYtRepoPath;
//     std::thread masterThread([ytRepoPath](){
//         const std::string configPath = ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/master.yson";

//         int argc = 3;
//         const char* argv[] = {"ytserver-master", "--config", configPath.c_str(), nullptr};
//         Master->Run(argc, argv);
//     });
//     std::thread dataNodeThread([ytRepoPath](){
//         const std::string configPath = ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/node.yson";

//         int argc = 3;
//         const char* argv[] = {"data-node", "--config", configPath.c_str(), nullptr};
//         DataNode->Run(argc, argv);
//     });
//     std::thread rpcProxyThread([ytRepoPath](){
//         const std::string configPath = ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/rpc_proxy.yson";

//         int argc = 3;
//         const char* argv[] = {"proxy", "--config", configPath.c_str(), nullptr};
//         RpcProxy->Run(argc, argv);
//     });
//     masterThread.detach();
//     dataNodeThread.detach();
//     rpcProxyThread.detach();

//     std::this_thread::sleep_for(std::chrono::seconds(10));
//     return true;
// }

// size_t getCurrentRSS() {
//     std::ifstream statm("/proc/self/statm");
//     size_t size = 0, resident = 0;

//     if (statm) {
//         // Read values from /proc/self/statm
//         statm >> size >> resident;  // We only need 'size' and 'resident'
//     } else {
//         std::cerr << "Failed to open /proc/self/statm" << std::endl;
//         return 0;  // Return 0 if file could not be opened
//     }

//     statm.close();
//     long page_size = sysconf(_SC_PAGESIZE); // Get system page size

//     return resident * page_size; // Calculate RSS in bytes
// }

// template<typename TRequest, typename TProxyMethod>
// void SendRequest(const std::string& methodName, const TRequest& request, TProxyMethod proxyMethod,
//         const NProtoBuf::RepeatedPtrField<TBasicString<char>>& attachments) {
//     const auto before_rss = getCurrentRSS();
//     // if (methodName != "GetChunkFragmentSet") return;
//     YT_LOG_INFO("LOOOG Sending %v, attachments size: %v", methodName, attachments.size());
//     std::cerr << "LOOOG Sending " << methodName << ", attachments size: " << attachments.size() << std::endl;
//     std::string rspMsg = "";
//     size_t retry = 0;
//     do {
//         if (retry) {
//             std::this_thread::sleep_for(std::chrono::seconds(1));
//         }
//         fuzzing::Timer t;
//         server = RpcProxy->WaitRpcServer();
//         auto channel = NYT::NRpc::CreateLocalChannel(server);
//         NYT::NApi::NRpcProxy::TApiServiceProxy proxy(channel);

//         auto req = (proxy.*proxyMethod)();
//         req->CopyFrom(request);
//         {
//             std::vector<NYT::NChunkClient::TBlock> blocks;
//             for (const auto& attachment : attachments) {
//                 blocks.emplace_back(
//                     NYT::TSharedRef::FromString(TString(attachment)));
//             }
//             req->Attachments().reserve(blocks.size());
//             for (const auto& block : blocks) {
//                 req->Attachments().push_back(block.Data);
//             }
//         }
//         auto rspOrError = NYT::NConcurrency::WaitFor(req->Invoke());
//         // YT_LOG_INFO("LOOOG %v took %v ms, response: %v", methodName, t.Reset(), rspOrError.GetMessage());
//         rspMsg = rspOrError.GetMessage();
//         // std::cerr << methodName << " took " <<  t.Reset() << " ms, response: " << rspMsg
//         //         << ", retry=" << retry
//         //         << ", attachments size:" << attachments.size() << ", real attach size: " << req->Attachments().size() << std::endl;

//         const auto after_rss = getCurrentRSS();
//         std::cerr << methodName << " took " <<  t.Reset() << " ms, response: " << rspMsg
//             << ", retry=" << retry
//             << ", attachments size:" << attachments.size() << ", real attach size: " << req->Attachments().size()
//             << ", before rss="<<before_rss << ", after_rss="<<after_rss << ", diff=" << (after_rss - before_rss)*1.0 / (1024 * 1024 * 1024) << "GB" << std::endl;
//         // std::this_thread::sleep_for(std::chrono::seconds(1));
//         ++retry;
//     } while (rspMsg == "Master is not connected");
// }


// int main(int argc, char* argv[]) {
//     GOOGLE_PROTOBUF_VERIFY_VERSION;

//     if (argc != 2) {
//         std::cerr << "Usage: " << argv[0] << " FILE_NAME" << std::endl;
//         return -1;
//     }

//     std::fstream str_input(argv[1], std::ios::in | std::ios::binary);
//     if (!str_input) {
//         std::cerr << "Failed to open file: " << argv[1] << std::endl;
//         return -1;
//     }

//     NYT::NApi::NRpcProxy::NProto::TRpcProxyFuzzerInput parsed_input;
//     if (!parsed_input.ParseFromIstream(&str_input)) {
//         std::cerr << "Failed to parse protobuf message." << std::endl;
//         return -1;
//     }
//     google::protobuf::TextFormat::Printer printer;
//     printer.SetUseUtf8StringEscaping(true);

//     TProtoStringType text_message;
//     std::cout << "Fuzzer input:\n";
//     if (!printer.PrintToString(parsed_input, &text_message)) {
//         std::cerr << "Failed to convert protobuf message to text." << std::endl;
//         return -1;
//     }

//     std::cout << text_message << std::endl;
//     Init();

//     for (const auto& request_with_attachments : parsed_input.requests()) {
//         const auto& request = request_with_attachments.request();
//         const auto& attachments = request_with_attachments.attachments();
//         switch (request.request_case()) {
//             case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kCreateNode:
//                 SendRequest("CreateNode", request.create_node(), &NYT::NApi::NRpcProxy::TApiServiceProxy::CreateNode, attachments);
//                 break;
//             case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kCreateObject:
//                 SendRequest("CreateObject", request.create_object(), &NYT::NApi::NRpcProxy::TApiServiceProxy::CreateObject, attachments);
//                 break;
//             case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kListNode:
//                 SendRequest("ListNode", request.list_node(), &NYT::NApi::NRpcProxy::TApiServiceProxy::ListNode, attachments);
//                 break;
//             case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kGetNode:
//                 SendRequest("GetNode", request.get_node(), &NYT::NApi::NRpcProxy::TApiServiceProxy::GetNode, attachments);
//                 break;
//             case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kReadFile:
//                 SendRequest("ReadFile", request.read_file(), &NYT::NApi::NRpcProxy::TApiServiceProxy::ReadFile, attachments);
//                 break;
//             case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteFile:
//                 SendRequest("WriteFile", request.write_file(), &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteFile, attachments);
//                 break;
//             case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kReadTable:
//                 SendRequest("ReadTable", request.read_table(), &NYT::NApi::NRpcProxy::TApiServiceProxy::ReadTable, attachments);
//                 break;
//             case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteTable:
//                 SendRequest("WriteTable", request.write_table(), &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteTable, attachments);
//                 break;
//             // case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteTableMeta:
//             //     SendRequest("WriteTableMeta", request.write_table_meta(), &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteTableMeta, attachments);
//             //     break;
//             default:
//                 break;
//         }
//     }

//     return 0;
// }

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

#include <yt/yt/client/api/rpc_proxy/api_service_proxy.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/connection_impl.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/yt/logging/logger.h>

#include <google/protobuf/text_format.h>
#include <sstream>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>
#include <unistd.h>  // for sysconf(_SC_PAGESIZE), fork, execv
#include <sys/types.h>
#include <sys/wait.h>
#include <set>

static const auto& Logger = NYT::NRpcProxy::RpcProxyLogger;
std::set<pid_t> childPIDs;
std::atomic_bool done = false;


bool Init() {
    const char* envYtRepoPath = std::getenv("YT_REPO_PATH");
    if (!envYtRepoPath || envYtRepoPath[0] == '\0') {
        std::cerr << "Environment variable YT_REPO_PATH is not set or empty." << std::endl;
        exit(1);
    }
    std::string ytRepoPath = envYtRepoPath;
    const char* envYtBuildPath = std::getenv("YT_BUILD_PATH");
    if (!envYtBuildPath || envYtBuildPath[0] == '\0') {
        std::cerr << "Environment variable YT_BUILD_PATH is not set or empty." << std::endl;
        exit(1);
    }
    std::string ytBuildPath = envYtBuildPath;

    auto forkAndExec = [](const std::string& programPath, const std::string& configPath) {
        pid_t pid = fork();
        if (pid == 0) {
            execl(programPath.c_str(), programPath.c_str(), "--config", configPath.c_str(), (char*)nullptr);
            std::cerr << "Failed to exec " << programPath << std::endl;
            exit(1);
        } else if (pid < 0) {
            std::cerr << "Fork failed for " << programPath << std::endl;
            exit(1);
        }
        return pid;
    };

    pid_t pid1 = forkAndExec(ytBuildPath + "/yt/yt/server/all/ytserver-master", ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/master.yson");
    pid_t pid2 = forkAndExec(ytBuildPath + "/yt/yt/server/all/ytserver-node", ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/node.yson");
    pid_t pid3 = forkAndExec(ytBuildPath + "/yt/yt/server/all/ytserver-proxy", ytRepoPath + "/yt/yt/server/all/fuzztests/distributed/rpc_proxy.yson");

    childPIDs = {pid1, pid2, pid3};
    auto monitorProcesses = [pid1, pid2, pid3]() {
        int status;
        while (!childPIDs.empty()) {
            pid_t pid = waitpid(-1, &status, 0);
            
            if (pid == -1) {
                std::cerr << "Error waiting for child process." << std::endl;
                break;
            } else if (pid > 0) {
                childPIDs.erase(pid);
                if (done) continue;
                if (WIFEXITED(status) || WIFSIGNALED(status)) {
                    std::cerr << "Process " << pid << " exited. Terminating main program." << std::endl;
                    // Kill remaining processes
                    kill(pid1, SIGKILL);
                    kill(pid2, SIGKILL);
                    kill(pid3, SIGKILL);
                    exit(1); // Exit the main program
                }
            }
        }
    };

    std::thread monitorThread(monitorProcesses);
    monitorThread.detach();

    std::this_thread::sleep_for(std::chrono::seconds(10));
    return true;
}

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

using namespace NYT;

class TMyRpcConfig : public TSingletonsConfig
{
};

DEFINE_REFCOUNTED_TYPE(TMyRpcConfig);

auto CreateRpcClient() {
    // auto proxyAddress = GetEnv("YT_PROXY");
    // if (proxyAddress.empty()) {
    //     THROW_ERROR_EXCEPTION("YT_PROXY environment variable must be set");
    // }
    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ProxyAddresses = {"127.0.0.1:9013"};
    connectionConfig->ProxyListUpdatePeriod = TDuration::Seconds(5);

    // todo: remove?
    // auto singletonsConfig = New<TMyRpcConfig>();
    // singletonsConfig->AddressResolver->EnableIPv4 = true;
    // singletonsConfig->AddressResolver->EnableIPv6 = false;
    // ConfigureSingletons(singletonsConfig);

    auto connection = New<NYT::NApi::NRpcProxy::TConnection>(connectionConfig, NYT::NApi::NRpcProxy::TConnectionOptions{});

    // auto token = NAuth::LoadToken();
    // if (!token) {
    //     THROW_ERROR_EXCEPTION("YT_TOKEN environment variable must be set");
    // }

    // NApi::TClientOptions clientOptions = NAuth::TAuthenticationOptions::FromToken(*token);
    // return connection->CreateClient();
    return connection->CreateChannelByAddress("127.0.0.1:9013");
}

template<typename TRequest, typename TProxyMethod>
void SendRequest(const std::string& methodName, const TRequest& request, TProxyMethod proxyMethod,
                 const NProtoBuf::RepeatedPtrField<TBasicString<char>>& attachments) {
    const auto before_rss = getCurrentRSS();
    YT_LOG_INFO("LOOOG Sending %v, attachments size: %v", methodName, attachments.size());
    std::cerr << "LOOOG Sending " << methodName << ", attachments size: " << attachments.size() << std::endl;
    std::string rspMsg = "";
    size_t retry = 0;
    do {
        if (retry) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        fuzzing::Timer t;
        // server = RpcProxy->WaitRpcServer();
        // auto channel = NYT::NRpc::CreateLocalChannel(server);

        // auto factory = NYT::New<TChannelFactory>();
        // auto cachingFactory = CreateCachingChannelFactory(factory, TDuration::MilliSeconds(500));
        // auto channel = cachingFactory->CreateChannel("");

        auto channel = CreateRpcClient();
        NYT::NApi::NRpcProxy::TApiServiceProxy proxy(channel);
        // auto proxy = client->CreateApiServiceProxy();

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
        rspMsg = rspOrError.GetMessage();

        const auto after_rss = getCurrentRSS();
        std::cerr << methodName << " took " << t.Reset() << " ms, response: " << rspMsg
                  << ", retry=" << retry
                  << ", attachments size:" << attachments.size() << ", real attach size: " << req->Attachments().size()
                  << ", before rss=" << before_rss << ", after_rss=" << after_rss << ", diff=" << (after_rss - before_rss) * 1.0 / (1024 * 1024 * 1024) << "GB" << std::endl;
        ++retry;
    } while (rspMsg == "Channel terminated");
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

    NYT::NApi::NRpcProxy::NProto::TRpcProxyFuzzerInput parsed_input;
    if (!parsed_input.ParseFromIstream(&str_input)) {
        std::cerr << "Failed to parse protobuf message." << std::endl;
        return -1;
    }
    google::protobuf::TextFormat::Printer printer;
    printer.SetUseUtf8StringEscaping(true);

    TProtoStringType text_message;
    std::cout << "Fuzzer input:\n";
    if (!printer.PrintToString(parsed_input, &text_message)) {
        std::cerr << "Failed to convert protobuf message to text." << std::endl;
        return -1;
    }

    std::cout << text_message << std::endl;
    Init();

    for (const auto& request_with_attachments : parsed_input.requests()) {
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
            case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kReadTable:
                SendRequest("ReadTable", request.read_table(), &NYT::NApi::NRpcProxy::TApiServiceProxy::ReadTable, attachments);
                break;
            case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteTable:
                SendRequest("WriteTable", request.write_table(), &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteTable, attachments);
                break;
            default:
                break;
        }
    }

    // int status;
    // while (wait(&status) > 0); // Wait for all child processes to finish
    done = true;
    std::cerr << "SUCCESS!!!" << std::endl;
        for (pid_t pid : childPIDs) {
        kill(pid, SIGTERM);  // Send SIGTERM to each child process
        int status;
        waitpid(pid, &status, 0); // Optionally wait for each to terminate
    }

    std::cerr << "All child processes terminated. Exiting now." << std::endl;
    return 0;
}
