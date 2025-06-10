#!/usr/bin/env python3

import logging
import sys

from google.protobuf.message import Message as Msg
from kafka import KafkaProducer
from opmonlib.conf import OpMonConf
from opmonlib.opmon_entry_pb2 import OpMonEntry
from opmonlib.publisher_base import OpMonPublisherBase
from opmonlib.utils import logging_log_level_from_str, setup_rich_handler


class OpMonPublisher(OpMonPublisherBase):
    """Tool for publishing operational monitoring metrics to kafka."""

    def __init__(self, conf: OpMonConf) -> None:
        """Construct the object to publish OpMon metrics to kafka."""
        super().__init__()
        self.log = logging.getLogger("OpMonPublisher")
        self.conf = conf
        if isinstance(self.conf.level, str):
            self.conf.level = logging_log_level_from_str(self.conf.level)
        self.log.setLevel(self.conf.level)
        self.log.addHandler(setup_rich_handler())

        if self.conf.opmon_type != "stream":
            self.log.error("Type must be stream to publish to kafka.")
            sys.exit(1)

        if self.conf.bootstrap == "":
            self.log.warning(
                "There is no boostrap provided, not initializing publisher to topic %s",
                self.conf.default_topic,
            )
            self.opmon_producer = None
            return

        self.default_topic = "monitoring." + self.conf.topic
        self.publisher = KafkaProducer(
            bootstrap_servers=conf.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )

        super().__post_init__()
        return

    def extract_key(self, opmon_entry: OpMonEntry) -> str:
        """Extract  the key from the OpMonEntry."""
        self.check_publisher()
        key = str(opmon_entry.origin.session)
        if opmon_entry.origin.application != "":
            key += "." + opmon_entry.origin.application
        for substructure_id in opmon_entry.origin.substructure:
            key += "." + substructure_id
        key += "/" + str(opmon_entry.measurement)
        return key

    def publish(
        self,
        message: Msg,
        custom_origin: dict[str, str] | None = None,
        level: int | str | None = None,
    ) -> None:
        """Send an OpMonEntry to Kafka."""
        if not isinstance(message, Msg):
            self.log.error("Passed message needs to be of type google.protobuf.message")
            return
        if not level:
            level = self.conf.level
        if isinstance(level, str):
            level = logging_log_level_from_str(level)
        if level < self.conf.level:
            return

        metric = self.to_entry(message=message, custom_origin=custom_origin)

        if len(metric.data) == 0:
            self.log.warning("OpMonEntry of type %s has no data", message.__name__)
            return

        target_topic = self.extract_topic(message)
        target_key = self.extract_key(metric)

        self.publisher.send(target_topic, value=metric, key=target_key)
        return
