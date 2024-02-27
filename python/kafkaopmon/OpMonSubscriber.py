#!/usr/bin/env python3

from kafka import KafkaConsumer
import threading 
import socket
import os
import re
import logging
import getpass

import opmonlib.opmon_entry_pb2 as entry
import google.protobuf.message as msg

class OpMonFunction :
    def __init(self,
               function,
               opmon_id : re.Pattern,
               measurement : re.Patern) :
        self.function = function
        self.opmon_id = opmon_id
        self.measurement = measurement

    def match(key : str) -> bool :
        temp_opmon_id,temp_measurement = key.split('/',1);
        if not self.opmon_id.match(temp_opmon_id) return False
        if not self.measurement.match(temp_measurment) return False
        return True

    def execute( e : entry.OpMonEntry ) :
        self.function(e)

class  OpMonSubscriber:
    def __init__(self, bootstrap, group_id=None, timeout_ms=500, topics=[]) :
        ## Options from configurations
        self.bootstrap = bootstrap
        self.group_id  = group_id
        self.timeout   = timeout_ms
        self.topics    = topics
        ## runtime options
        self.running = False
        self.functions = dict()
        self.thread = threading.Thread(target=self.message_loop)

    def default_id(self) -> str:
        node = socket.gethostname()
        user = getpass.getuser()
        process = os.getpid()
        thread = threading.get_ident()
        id = "{}-{}-{}-{}".format(node, user, process, thread)
        return id

    def add_callback(self,
                     name, function, opmon_id = '.*', measurement = '.*') -> bool:
        if ( name in self.functions ) : return False
       
        was_running = self.running
        if (was_running) : self.stop()

        f = OpMonFunction( function = function,
                           opmon_id = re.compile(opmon_id)
                           measurement = re.compile(measurement) )
        
        self.functions[name] = f

        if (was_running) : self.start()
        return True

    def clear_callbacks(self):
        if ( self.running ) :
            self.stop()
        self.functions.clear()

    def remove_callback(self, name) -> bool:
        if ( name not in sef.functions.keys() ) : return False

        was_running = self.running
        if (was_running) : self.stop()

        self.functions.pop(name)

        if ( was_running and len(self.functions)>0 ) : self.start()
        return True

    def start(self):
        logging.info("Starting run")
        self.running = True
        self.thread.start()

    def stop(self) :
        self.running = False
        self.thread.join()

    def message_loop(self) :
        if not self.group : group_id = self.default_id()
        else: group_id = self.group

        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap,
                                 group_id=group_id, 
                                 client_id=self.default_id(),
                                 consumer_timeout_ms=self.timeout)
        
        topics = self.topics
        if len(topics) == 0 : topics = ["ers_stream"]
        consumer.subscribe(["monitoring." + s for s in topics])

        logging.info("ID:", group_id, "running with functions:", *self.functions.keys())

        while ( self.running ) :
            try:
                message_it = iter(consumer)
                message = next(message_it)
                timestamp = message.timestamp
                key = message.key.decode('ascii')
                ## The key from the message is binary
                ## In order to correctly match an ascii regex, we have to convert
                
                for function in self.functions.values() :
                    if function.match(key) :
                        e = entry.OpMonEntry()
                        e.ParseFromString( message.value )
                        function.execute(e)
                
            except msg.DecodeError :
                logging.error("Could not parse message")
            except StopIteration :
                pass
            except Exception as e:
                logging.error(e)

        logging.info("Stop run")
