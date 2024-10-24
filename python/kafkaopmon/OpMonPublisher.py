#!/usr/bin/env python3

from opmonlib.opmon_entry_pb2 import OpMonValue, OpMonId, OpMonEntry 
from google.protobuf.message import Message as msg
from google.protobuf.descriptor import FieldDescriptor as fd
from google.protobuf.timestamp_pb2 import Timestamp
from erskafka.ERSKafkaLogHandler import ERSKafkaLogHandler

import os
import logging

from datetime import datetime
from kafka import KafkaProducer
from typing import Optional

class OpMonPublisher:
    def __init__(
                    self, 
                    default_topic:str,
                    bootstrap:str = "", # When no bootstrap is provided, there will be nothing published
                    ers_session:str = "session_tester",
                    log_level:int = logging.DEBUG
    ) -> None:
        # Set up text logging
        self.ers_session = ers_session
        self.log = logging.getLogger("OpMonPublisher")
        self.log.setLevel(log_level)

        # Initialize object parameters
        self.bootstrap = bootstrap
        self.default_topic = "monitoring." + default_topic

        ## Options from configurations
        if bootstrap == "":
            self.log.warning(f"There is no boostrap provided, not initializing publisher to topic {default_topic}")
            self.opmon_producer = None
            return None

        # Setup the ERS logging
        self.ersHandler = ERSKafkaLogHandler(
            session = self.ers_session,
            kafka_address = self.bootstrap,
            kafka_topic = "ers_stream"
        )
        self.log.addHandler(self.ersHandler)

        # Setup the opmon publisher
        self.opmon_producer = KafkaProducer(
            bootstrap_servers = self.bootstrap,
            value_serializer = lambda v: v.SerializeToString(),
            key_serializer = lambda k: str(k).encode('utf-8')
        )

    def publish(
        self,
        session:str,
        application:str,
        message:msg,
        custom_origin:Optional[dict[str,str]] = {},
        substructure:Optional[list[str]] = []
    ) -> None:
        """Create an OpMonEntry and send it to Kafka."""
        # If there is no publisher, do not send anything
        t = Timestamp()
        time = t.GetCurrentTime()

        # Pre-map checks
        if not self.opmon_producer:
            self.log.warning(f"An improperly initialized OpMonProducer with topic {self.default_topic} has been used, nothign will be published.")
            return
        if not isinstance(message, msg):
            raise ValueError("This is not an accepted publish value, it needs to be of type google.protobuf.message")

        for key, value in custom_origin.items():
            if type(value) != "str":
                try:
                    custom_origin[key] = str(value)
                except:
                    raise TypeError(f"custom_origin[{key}] is not a string and cannot be converted to one.")

        data_dict = self.map_message(message)
        opmon_id = OpMonId(
            session = session,
            application = application,
            substructure = substructure
        )
        opmon_entry = OpMonEntry(
            time = time,
            origin = opmon_id,
            custom_origin = custom_origin,
            measurement = message.DESCRIPTOR.full_name,
            data = data_dict,
        )

        # Pre publish check - if the message has no known message types
        if len(opmon_entry.data) == 0:
            self.log.warning(f"OpMonEntry of type {message.__name__} has no data")
            return  

        target_topic = self.extract_topic(message)
        target_key = self.extract_key(opmon_entry)
        self.opmon_producer.send(
            target_topic,
            value = opmon_entry,
            key = target_key
        )
        return

    def extract_topic(self, message:msg) -> str:
        if not self.opmon_producer:
            self.log.warning(f"An improperly initialized OpMonProducer with topic {self.default_topic} has been used, nothign will be published.")
            return None
        return self.default_topic

    def extract_key(self, opmon_entry:OpMonEntry) -> str:
        if not self.opmon_producer:
            self.log.warning(f"An improperly initialized OpMonProducer with topic {self.default_topic} has been used, nothign will be published.")
            return None
        key = str(opmon_entry.origin.session) 
        if (opmon_entry.origin.application != ""):
            key += "." + opmon_entry.origin.application
        for substructureID in opmon_entry.origin.substructure:
            key += "." + substructureID
        key += '/' + str(opmon_entry.measurement)
        return key

    def map_message(self, message:msg, top_block:str=""):
        message_dict = {}
        for name, descriptor in message.DESCRIPTOR.fields_by_name.items():
            if descriptor.label == fd.LABEL_REPEATED:
                continue # We don't want to keep repeated values as this doens't work for influxdb as there is no way to store repeated values
            elif descriptor.cpp_type == fd.CPPTYPE_MESSAGE:
                # Prepend the name of the nested message to the attribute name
                top_block += name + "."
                message_dict = message_dict | self.map_message(getattr(message, name), top_block)
            else:
                message_dict[top_block + name] = self.map_entry(getattr(message, name), descriptor.cpp_type)
        return message_dict

    def map_entry(self, value, field_type:int) -> OpMonValue:
        formatted_OpMonValue = OpMonValue()
        match field_type:
            case fd.CPPTYPE_INT32:
                formatted_OpMonValue.int4_value = value
            case fd.CPPTYPE_INT64:
                formatted_OpMonValue.int8_value = value
            case fd.CPPTYPE_UINT32:
                formatted_OpMonValue.uint4_value = value
            case fd.CPPTYPE_UINT64:
                formatted_OpMonValue.uint8_value = value
            case fd.CPPTYPE_DOUBLE:
                formatted_OpMonValue.double_value = value
            case fd.CPPTYPE_FLOAT:
                formatted_OpMonValue.float_value = value
            case fd.CPPTYPE_BOOL:
                formatted_OpMonValue.boolean_value = value
            case fd.CPPTYPE_STRING:
                formatted_OpMonValue.string_value = value
            # Ignore unknown types.
        return formatted_OpMonValue