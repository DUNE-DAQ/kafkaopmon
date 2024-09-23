#!/usr/bin/env python3

import os
import socket
import inspect
import opmonlib.opmon_entry_pb2 as entry
import google.protobuf.message as msg
from datetime import datetime
from kafka import KafkaProducer
import time
from typing import Optional

class SeverityLevel(IntEnum):
    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    FATAL = auto()

class  OpMonPublisher:
    def __init__(
                    self, 
                    topic:str,
                    bootstrap:str = "monkafka.cern.ch:30092", # Removed for if we don't want to use OpMon (i.e. for ssh-standalone)
                    application_name:str = "python",
                    package_name:str = "unknown"
    ) -> None:
        ## Options from configurations
        self.application_name = application_name
        self.package_name = package_name
        self.bootstrap = bootstrap
        if not topic.startswith('monitoring.'):
            topic = 'monitoring.' + topic
        self.topic = topic

        ## runtime options
        self.log = logging.getLogger("OpMonPublisher")
        self.log.info("Starting Kafka producer")
        self.producer = KafkaProducer(
                                        bootstrap_servers=self.bootstrap,
                                        value_serializer=lambda v: v.SerializeToString(),
                                        key_serializer=lambda k: str(k).encode('utf-8')
        )
        self.log.info("Initialized Kafka producer")        

    def publish(
        self,
        session:str,
        application:str,
        message:google.protobuf.message, # Is this legal?
        custom_origin:Optional[dict[str:str]] = {},
    ):
        """Create and issue and send to to the Kafka."""
        opmon_id = entry.OpMonID(
            session = session,
            application = application,
            substructure = "" # For now, can be extended later
        )

        opmon_metric = entry.OpMonEntry(
            time = time.time_ns(),
            origin = opmon_id,
            custom_origin = custom_origin,
            measurement = message.TypeName(),
            data = map_message(message)
        )
        return self.producer.send(opmon_metric)

    def map_message(self, message:google.protobuf.message):
        message_dict = {}
        for name, descriptor in message.fields_by_name:# RETURNTOME - was looking at fields_by_name
            message_dict[name] = map_entry(descriptor, message.getattr(name)) 
        return message_dict

    def map_entry(self, variable_type, variable):
        formatted_OpMonValue = OpMonValue() 
        match variable_type:
            case "TYPE_INT32":
                formatted_OpMonValue.int4_value = variable
            case "TYPE_INT64":
                formatted_OpMonValue.int8_value = variable
            case "TYPE_UINT32":
                formatted_OpMonValue.uint4_value = variable
            case "TYPE_UINT64":
                formatted_OpMonValue.uint8_value = variable
            case "TYPE_DOUBLE":
                formatted_OpMonValue.double_value = variable
            case "TYPE_FLOAT":
                formatted_OpMonValue.float_value = variable
            case "TYPE_BOOL":
                formatted_OpMonValue.boolean_value = variable
            case "TYPE_STRING":
                formatted_OpMonValue.string_value = variable
            case _:
                raise ValueError("Variable is of a non-supported type.")
        return formatted_OpMonValue