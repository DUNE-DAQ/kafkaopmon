/**
 * @file OpMonPublisher_test.cxx Test application that tests invalid constructions of OpMonPublisher
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#define BOOST_TEST_MODULE opmon_publisher_test // NOLINT

#include "boost/test/unit_test.hpp"

#include <kafkaopmon/OpMonPublisher.hpp>

using namespace dunedaq::kafkaopmon;
using namespace dunedaq::opmon;

BOOST_AUTO_TEST_SUITE(OpMonPublisher_Test)

BOOST_AUTO_TEST_CASE(Invalid_Creation) {

  nlohmann::json conf;

  BOOST_CHECK_THROW( OpMonPublisher p(conf),
		     MissingParameter  );

  conf["bootstrap"] = "invalid.address.none:1234";

  BOOST_CHECK_NO_THROW( OpMonPublisher p(conf) );

  // this is a bit annoyting, but it is what it is
  // Kakfa creates a producer but the check of the correctness is done asynchronously
  // As a result, even with an invalid address, the best we get in a very silent error message
  
}

BOOST_AUTO_TEST_SUITE_END()




