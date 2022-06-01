cmake_minimum_required(VERSION 3.12)
project(kafkaopmon VERSION 1.3.2)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

find_package(opmonlib REQUIRED)
find_package(CPR REQUIRED)
find_package(CURL REQUIRED)
find_package(RdKafka REQUIRED)
find_package(ers REQUIRED)     
find_package(Boost COMPONENTS program_options REQUIRED)



daq_add_library(*.cpp LINK_LIBRARIES opmonlib::opmonlib)


##############################################################################
# Plugins

daq_add_plugin(kafkaOpmonService duneOpmonService LINK_LIBRARIES opmonlib::opmonlib RdKafka::rdkafka RdKafka::rdkafka++ ers::ers kafkaopmon)
target_include_directories(kafkaopmon_kafkaOpmonService_duneOpmonService PUBLIC)
daq_add_application(kafka_opmon_consumer kafka_opmon_consumer.cxx LINK_LIBRARIES opmonlib::opmonlib RdKafka::rdkafka++ CURL::libcurl ers::ers ${CPR_LIBRARIES} ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories(kafka_opmon_consumer PUBLIC ${CPR_INCLUDE_DIR})


##############################################################################
# No integration tests written

##############################################################################
# No unit tests written

##############################################################################
# test application

daq_add_application( test_flattener test_flattener.cxx TEST LINK_LIBRARIES kafkaopmon)

daq_install()

