#!/usr/bin/env python3

import logging
import sys

from google.protobuf.message import Message as Msg
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaProducer
from opmonlib.conf import OpMonConf
from opmonlib.utils import (
    extract_key,
    extract_topic,
    log_level_from_str,
    setup_rich_handler,
    to_entry,
)


class OpMonPublisher:
    """Tool for publishing operational monitoring metrics to kafka."""

    def __init__(self, conf: OpMonConf) -> None:
        """Construct the object to publish OpMon metrics to kafka."""
        self.log = logging.getLogger("OpMonPublisher")
        self.conf = conf
        if isinstance(self.conf.level, str):
            self.conf.level = log_level_from_str(self.conf.level)
        self.log.setLevel(self.conf.level)
        self.log.addHandler(setup_rich_handler())

        if self.conf.opmon_type != "stream":
            self.log.error("Type must be stream to publish to kafka.")
            sys.exit(1)

        self.default_topic = "monitoring." + self.conf.topic

        self.publisher = KafkaProducer(
            bootstrap_servers=conf.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )
        return

    def publish(
        self,
        session: str,
        application: str,
        message: Msg,
        custom_origin: dict[str, str] | None = None,
        substructure: list[str] | None = None,
        level: int | str | None = None,
    ) -> None:
        """Send an OpMonEntry to Kafka."""
        if not isinstance(message, Msg):
            self.log.error("Passed message needs to be of type google.protobuf.message")
            return
        if not level:
            level = self.conf.level
        if isinstance(level, str):
            level = log_level_from_str(level)
        if level < self.conf.level:
            return
        metric = to_entry(
            session=session,
            application=application,
            message=message,
            custom_origin=custom_origin,
            substructure=substructure,
            t=Timestamp().GetCurrentTime(),
        )
        target_topic = extract_topic(message)
        target_key = extract_key(metric)

        self.publisher.send(target_topic, value=metric, key=target_key)
        return
