/**
 * @file stream_OpMoFacility_test.cxx Test application that tests and demonstrates
 * basic functionality of the the streamOpMonFacility
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "opmonlib/OpMonFacility.hpp"
#include "opmonlib/Utils.hpp"
#include "opmonlib/info/test.pb.h"

#define BOOST_TEST_MODULE stream_opmon_facility_test // NOLINT

#include "boost/test/unit_test.hpp"

using namespace dunedaq::opmonlib;
using namespace dunedaq::opmon;

BOOST_AUTO_TEST_CASE(Invalid_Creation) {

  // failure due to wrong formatting
  BOOST_CHECK_THROW( auto service = makeOpMonFacility("stream://bla_bla"),
		     OpMonFacilityCreationFailed );
  
  BOOST_CHECK_NO_THROW( auto service = makeOpMonFacility("stream://test.website.com:5005/no_topic") );
}
