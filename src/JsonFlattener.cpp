// * This is part of the DUNE DAQ Application Framework, copyright 2020.
// * Licensing/copyright details are in the COPYING file that you should have received with this code.
#include <ctime>

#include "JsonFlattener.hpp"

#include "opmonlib/JSONTags.hpp"

namespace dunedaq
{

  using opmonlib::JSONTags;

  namespace kafkaopmon
    {
      JsonFlattener::JsonFlattener(const nlohmann::json& j) {
	if ( j.size() != 2 or j.count(JSONTags::parent) != 1 or j.count(JSONTags::tags) != 1 ) {
	  throw OpmonJSONValidationError(ERS_HERE, "Root key '"+std::string(JSONTags::parent)+"' not found" );
	}
	
	if ( j[JSONTags::parent].size() != 1) {
	  throw OpmonJSONValidationError(ERS_HERE, "Expected 1 top-level entry, found " + std::to_string(j[JSONTags::parent].size()) );
	}
	
	// even easier with structured bindings (C++17)
	for (auto& [key, value] : j[JSONTags::tags].items()) {
	  m_tags[key] = value;
	}
	
	parse_json("", j[JSONTags::parent]);
      }

      void JsonFlattener::parse_json( std::string path,
				 const nlohmann::json& j) {
	
	for (auto& [key, obj] : j.items()) {

	  auto objpath = (path.size() ? path+m_separator+key : key);

	  // validate meta filelds, i.e.
	  if (not obj.is_object())
	    throw OpmonJSONValidationError(ERS_HERE, key + " is not an object");

	  // if properties are here, process them
	  if (obj.count(JSONTags::properties)) {
	    // Loop over property structures
	    for (auto& pstruct : obj[JSONTags::properties].items()) {
	      
	      // Make sure that this is a json object
	      if (not pstruct.value().is_object())
		throw OpmonJSONValidationError(ERS_HERE, pstruct.key() + " is not an object");
	      
	      // And that it contains the required fields
	      if (pstruct.value().count(JSONTags::time) == 0 or pstruct.value().count(JSONTags::data) == 0)
		throw OpmonJSONValidationError(ERS_HERE, pstruct.key() + " has no " +
					       JSONTags::time + " or " +
					       JSONTags::data + " tag");
	      
	      // Check for presence of stubstructures
	      std::vector<std::string> sub_structs;
	      for (auto& pobj : pstruct.value().at(JSONTags::data).items()) {
		if (pobj.value().is_object()) {
		  sub_structs.push_back(pobj.key());
		} 
	      }
	      if (sub_structs.size()) {
		throw OpmonJSONValidationError(ERS_HERE, pstruct.key() + " contains substructures"); 
	      }
	      
	      nlohmann::json entry;
	      auto & temp_tags = entry["tags"] = m_tags;
	      temp_tags["source_id"] = objpath;
	      entry["measurement"] = pstruct.key();
	      entry["fields"] = pstruct.value().at(JSONTags::data);
	      std::time_t seconds = pstruct.value().at(JSONTags::time).get<std::time_t>();
	      auto gmt =gmtime(& seconds);
	      char time_c_string[80];
	      strftime( time_c_string, 80, "%Y-%m-%dT%H:%M:%SZ", gmt );
	      std::string time_string(time_c_string);
	      entry["time"] = time_string;
	      m_components.push_back(entry);
	    }
	  }
	  
	  // and then go through children
	  if (obj.count(JSONTags::children)) {
	    // Recurse over children
	    this->parse_json( objpath, obj[JSONTags::children] );
	  }
	}
      }
      
  } // namespace kafkaopmon
} // namespace dunedaq

