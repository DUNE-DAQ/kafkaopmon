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
                    bootstrap:str = "monkafka.cern.ch:30092", # Removed for if we don't want to use OpMon (i.e. for ssh-standalone)
                    ers_session:str = "session_tester"
    ) -> None:
        ## Options from configurations
        self.bootstrap = bootstrap
        if not default_topic.startswith('monitoring.'):
            default_topic = 'monitoring.' + default_topic
        self.default_topic = default_topic

        # Setup the ERS configuration in case 
        self.ers_session = ers_session
        self.log = logging.getLogger("OpMonPublisher")
        self.log.setLevel(logging.DEBUG)
        self.ersHandler = ERSKafkaLogHandler(
            session = self.ers_session,
            kafka_address = self.bootstrap,
            kafka_topic = "ers_stream"
        )
        self.streamHandler = logging.StreamHandler()
        self.log.addHandler(self.ersHandler)
        self.log.addHandler(self.streamHandler)

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
        custom_origin:Optional[dict[str,str]] = {"": ""},
        substructure:Optional[str] = "",
    ) -> None:
        """Create an OpMonEntry and send it to Kafka."""
        t = Timestamp()
        time = t.GetCurrentTime()

        # Pre-map checks
        if not isinstance(message, msg):
            self.log.error("The passed entry is not a google.protobuf.message")
            raise ValueError("This is not an accepted publish value, it needs to be of type google.protobuf.message")
        if len(message.ListFields()) == 0:
            self.log.warning(f"OpMonEntry of type {message.__name__} has no data")
            return

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
            measurement = message.DESCRIPTOR.name,
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
        return self.default_topic

    def extract_key(self, opmon_entry:OpMonEntry) -> str:
        key = str(opmon_entry.origin.session) 
        if (opmon_entry.origin.application != ""):
            key += "." + opmon_entry.origin.application
        for substructureID in opmon_entry.origin.substructure:
            key += "." + substructureID
        key += '/' + str(opmon_entry.measurement)
        return key

    def map_message(self, message:msg):
        message_dict = {}
        for name, descriptor in message.DESCRIPTOR.fields_by_name.items():
            if descriptor.label == fd.LABEL_REPEATED:
                continue # We don't want to keep repeated values as this doens't work for influxdb as there is no way to store repeated values
            message_dict = self.map_entry(name, getattr(message, name), descriptor.cpp_type, message_dict)
        return message_dict

    def map_entry(self, attribute_name:str, value, field_type:int, message_dict:dict) -> dict:
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
            case fd.CPPTYPE_STRING:
                formatted_OpMonValue.string_value = value
            case fd.CPPTYPE_MESSAGE:
                message_dict = message_dict | self.map_message(value)
        # Ignore unknown types.
        if field_type != fd.CPPTYPE_MESSAGE:
            message_dict[attribute_name] = formatted_OpMonValue
        return message_dict