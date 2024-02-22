#include <fstream>
#include <iostream>
#include <string>

#include <google/protobuf/text_format.h>

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>


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
    if (!printer.PrintToString(bin_message, &text_message)) {
        std::cerr << "Failed to convert protobuf message to text." << std::endl;
        return -1;
    }

    std::cout << text_message << std::endl;

    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}
