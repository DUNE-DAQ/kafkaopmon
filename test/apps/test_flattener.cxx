/**
 * @brief Using namespace for convenience
 */
#include "JsonFlattener.hpp"

#include <fstream>

using json = nlohmann::json;

int main(int argc, char const *argv[])
{

    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " <opmonlib sample>.json" << std::endl;
        exit(-1);
    }

    std::ifstream file((argv[1]));
    json j = json::parse(file);

    std::cout << j << std::endl;

    std::cout << "------" << std::endl;
    auto iqb = dunedaq::kafkaopmon::JsonFlattener(j);

    for( auto item : iqb.get()) {
        std::cout << item << std::endl;
    }

    /* code */
    return 0;
}
