cmake_minimum_required(VERSION 3.12)
project(kafkaopmon VERSION 1.3.0)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

find_package(opmonlib REQUIRED)
find_package(CPR)
find_package(CURL)
find_package(RdKafka REQUIRED)

set(CONSUMER_DEPENDENCIES ers::ers nlohmann_json::nlohmann_json ${CPR_LIBRARIES})

##############################################################################
# Plugins

daq_add_plugin(kafkaOpmonService duneOpmonService LINK_LIBRARIES opmonlib::opmonlib CURL::libcurl RdKafka::rdkafka RdKafka::rdkafka++ ${CPR_LIBRARIES})

daq_add_application(kafkaOpmonConsumer cprExport.cxx LINK_LIBRARIES opmonlib::opmonlib CURL::libcurl RdKafka::rdkafka RdKafka::rdkafka++ ${CPR_LIBRARIES} )


#target_include_directories(kafkaopmon_kafkaOpmonConsumer PUBLIC ${CPR_INCLUDE_DIRS})
#daq_add_library(cprExport.cxx LINK_LIBRARIES ${CPR_INCLUDE_DIRS})

target_include_directories(${PROJECT_NAME}_kafkaOpmonService_duneOpmonService PUBLIC ${CPR_INCLUDE_DIRS})

##############################################################################
# No integration tests written

##############################################################################
# No unit tests written

##############################################################################

daq_install()

