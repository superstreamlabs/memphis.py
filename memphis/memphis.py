# Copyright 2021-2022 The Memphis Authors
# Licensed under the MIT License (the "License");
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# This license limiting reselling the software itself "AS IS".

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import random
import json

import nats as broker

import uuid
from threading import Timer
import asyncio

import memphis.retention_types as retention_types
import memphis.storage_types as storage_types


class set_interval():
    def __init__(self, func, sec):
        def func_wrapper():
            self.t = Timer(sec, func_wrapper)
            self.t.start()
            func()
        self.t = Timer(sec, func_wrapper)
        self.t.start()

    def cancel(self):
        self.t.cancel()


class Memphis:

    def __init__(self):
        self.is_connection_active = False
    
    async def connect(self, host, username, connection_token, port=6666, reconnect=True, max_reconnect=10, reconnect_interval_ms=1500, timeout_ms=15000):
        """Creates connection with Memphis.
        Args:
            host (str): memphis host.
            username (str): user of type root/application.
            connection_token (str): broker token.
            port (int, optional): port. Defaults to 6666.
            reconnect (bool, optional): whether to do reconnect while connection is lost. Defaults to True.
            max_reconnect (int, optional): The reconnect attempt. Defaults to 3.
            reconnect_interval_ms (int, optional): Interval in miliseconds between reconnect attempts. Defaults to 200.
            timeout_ms (int, optional): connection timeout in miliseconds. Defaults to 15000.
        """
        self.host = self.__normalize_host(host)
        self.username = username
        self.connection_token = connection_token
        self.port = port
        self.reconnect = reconnect
        self.max_reconnect = 9 if max_reconnect > 9 else max_reconnect
        self.reconnect_interval_ms = reconnect_interval_ms
        self.timeout_ms = timeout_ms
        self.connection_id = self.__generateConnectionID()
        try:
            self.broker_manager = await broker.connect(servers=self.host+":"+str(self.port), 
                                                    allow_reconnect=self.reconnect, 
                                                    reconnect_time_wait=self.reconnect_interval_ms/1000, 
                                                    connect_timeout=self.timeout_ms/1000, 
                                                    max_reconnect_attempts=self.max_reconnect,
                                                    token=self.connection_token, 
                                                    name=self.connection_id + "::" + self.username, max_outstanding_pings=1)
            
            self.broker_connection = self.broker_manager.jetstream()
            self.is_connection_active = True
        except Exception as e:
            raise Exception(e)

    async def factory(self, name, description=""):
        """Creates a factory.
        Args:
            name (str): factory name.
            description (str, optional): factory description(optional).
        Raises:
            Exception: _description_
            Exception: _description_
        Returns:
            object: factory
        """
        try:
            if not self.is_connection_active:
                raise Exception("Connection is dead")
            createFactoryReq = {
                    "factory_name": name,
                    "factory_description": description
                }
            create_factory_req_bytes = json.dumps(createFactoryReq, indent=2).encode('utf-8')
            err_msg =  await self.broker_manager.request("$memphis_factory_creations", create_factory_req_bytes)
            err_msg = err_msg.data.decode("utf-8") 

            if err_msg != "":
                raise Exception(err_msg)       
            return Factory(self, name)

        except Exception as e:
            if str(e).find('already exist') != -1:
                return Factory(self, name.lower())
            else:
                raise Exception(e)

    async def station(self, name, factory_name, retention_type=retention_types.MAX_MESSAGE_AGE_SECONDS, retention_value=604800, storage_type=storage_types.FILE, replicas=1, dedup_enabled=False, dedup_window_ms=0):
        """Creates a station.
        Args:
            name (str): station name.
            factory_name (str): factory name to link the station with.
            retention_type (str, optional): retention type: message_age_sec/messages/bytes . Defaults to "message_age_sec".
            retention_value (int, optional): number which represents the retention based on the retention_type. Defaults to 604800.
            storage_type (str, optional): persistance storage for messages of the station: file/memory. Defaults to "file".
            replicas (int, optional):number of replicas for the messages of the data. Defaults to 1.
            dedup_enabled (bool, optional): whether to allow dedup mecanism, dedup happens based on message ID. Defaults to False.
            dedup_window_ms (int, optional): time frame in which dedup track messages. Defaults to 0.
        Returns:
            object: station
        """
        try:
            if not self.is_connection_active:
                raise Exception("Connection is dead")
             
            createStationReq = {
                "name": name,
                "factory_name": factory_name,
                "retention_type": retention_type,
                "retention_value": retention_value,
                "storage_type": storage_type,
                "replicas": replicas,
                "dedup_enabled": dedup_enabled,
                "dedup_window_in_ms": dedup_window_ms
                }    
            create_station_req_bytes = json.dumps(createStationReq, indent=2).encode('utf-8')
            err_msg =  await self.broker_manager.request("$memphis_station_creations", create_station_req_bytes)
            err_msg = err_msg.data.decode("utf-8") 

            if err_msg != "":
                raise Exception(err_msg)              
            return Station(self, name)

        except Exception as e:
            if str(e).find('already exist') != -1:
                return Station(self, name.lower())
            else:
                raise Exception(e)

    async def close(self):
        """Close Memphis connection.
        """
        try:
            if self.is_connection_active:
                await self.broker_manager.close()
                self.broker_manager = None
                self.connection_id = None
                self.is_connection_active = False
        except:
            return

    def __generateConnectionID(self):
        lst = [random.choice('0123456789abcdef') for n in range(24)]
        s = "".join(lst)
        return s

    def __normalize_host(self, host):
        if (host.startswith("http://")):
            return host.split("http://")[1]
        elif (host.startswith("https://")):
            return host.split("https://")[1]
        else:
            return host

    async def producer(self, station_name, producer_name):
        """Creates a producer.
        Args:
            station_name (str): station name to produce messages into.
            producer_name (str): name for the producer.
        Raises:
            Exception: _description_
            Exception: _description_
        Returns:
            _type_: _description_
        """
        try:
            if not self.is_connection_active:
                raise Exception("Connection is dead")
                    
            createProducerReq = {
                "name": producer_name,
                "station_name": station_name,
                "connection_id": self.connection_id,
                "producer_type": "application"
                }    
            create_producer_req_bytes = json.dumps(createProducerReq, indent=2).encode('utf-8')
            err_msg =  await self.broker_manager.request("$memphis_producer_creations", create_producer_req_bytes)
            err_msg = err_msg.data.decode("utf-8") 

            if err_msg != "":
                raise Exception(err_msg)              
            return Producer(self, producer_name, station_name)

        except Exception as e:
            raise Exception(e)


    async def consumer(self, station_name, consumer_name, consumer_group="", pull_interval_ms=1000, batch_size=10, batch_max_time_to_wait_ms=5000, max_ack_time_ms=30000, max_msg_deliveries=10):
        """Creates a consumer.
        Args:.
            station_name (str): station name to consume messages from.
            consumer_name (str): name for the consumer.
            consumer_group (str, optional): consumer group name. Defaults to the consumer name.
            pull_interval_ms (int, optional): interval in miliseconds between pulls. Defaults to 1000.
            batch_size (int, optional): pull batch size. Defaults to 10.
            batch_max_time_to_wait_ms (int, optional): max time in miliseconds to wait between pulls. Defaults to 5000.
            max_ack_time_ms (int, optional): max time for ack a message in miliseconds, in case a message not acked in this time period the Memphis broker will resend it. Defaults to 30000.
            max_msg_deliveries (int, optional): max number of message deliveries, by default is 10.
        Returns:
            object: consumer
        """
        try:
            if not self.is_connection_active:
                raise Exception("Connection is dead")
            cg = consumer_name if not consumer_group else consumer_group
     
            createConsumerReq = {
                'name': consumer_name,
                "station_name": station_name,
                "connection_id": self.connection_id,
                "consumer_type": 'application',
                "consumers_group": consumer_group,
                "max_ack_time_ms": max_ack_time_ms,
                "max_msg_deliveries": max_msg_deliveries
                }

            create_consumer_req_bytes = json.dumps(createConsumerReq, indent=2).encode('utf-8')
            err_msg =  await self.broker_manager.request("$memphis_consumer_creations", create_consumer_req_bytes)
            err_msg = err_msg.data.decode("utf-8") 

            if err_msg != "":
                raise Exception(err_msg)              
            return Consumer(self, station_name, consumer_name, cg, pull_interval_ms, batch_size, batch_max_time_to_wait_ms, max_ack_time_ms, max_msg_deliveries)
        
        except Exception as e:
            raise Exception(e)


class Factory:
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name.lower()

    async def destroy(self):
        """Destroy the factory.
        """
        try:
            nameReq = {
                "factory_name":self.name
            }
            factory_name = json.dumps(nameReq, indent=2).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_factory_destructions', factory_name)
            error = res.data.decode('utf-8')
            if error != "" and not "not exist" in error:
                raise Exception(error)
        except Exception as e:
            raise Exception(e)
 


class Station:
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name.lower()

    async def destroy(self):
        """Destroy the station.
        """
        try:
            nameReq = {
                "station_name":self.name
            }
            station_name = json.dumps(nameReq, indent=2).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_station_destructions', station_name)
            error = res.data.decode('utf-8')
            if error != "" and not "not exist" in error:
                raise Exception(error)
        except Exception as e:
            raise Exception(e)


class Producer:
    def __init__(self, connection, producer_name, station_name):
        self.connection = connection
        self.producer_name = producer_name
        self.station_name = station_name

    async def produce(self, message, ack_wait_sec=15):
        """Produces a message into a station.
        Args:
            message (Uint8Array): message to send into the station.
            ack_wait_sec (int, optional): max time in seconds to wait for an ack from memphis. Defaults to 15.
        Raises:
            Exception: _description_
            Exception: _description_
        """
        try:
            await self.connection.broker_connection.publish(self.station_name + ".final", message, timeout=ack_wait_sec, headers={
                "Nats-Msg-Id": str(uuid.uuid4()), "producedBy": self.producer_name, "connectionId": self.connection.connection_id})
        except Exception as e:
            if hasattr(e, 'status_code') and e.status_code == '503':
                raise Exception(
                    "Produce operation has failed, please check whether Station/Producer are still exist")
            else:
                raise Exception(e)

    async def destroy(self):
        """Destroy the producer.
        """
        try:
            destroyProducerReq = {
                "name": self.producer_name,
                "station_name": self.station_name
            }
            
            producer_name = json.dumps(destroyProducerReq).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_producer_destructions', producer_name)
            error = res.data.decode('utf-8')
            if error != "" and not "not exist" in error:
                raise Exception(error)
        except Exception as e:
            raise Exception(e)


class Consumer:
    def __init__(self, connection, station_name, consumer_name, consumer_group, pull_interval_ms, batch_size, batch_max_time_to_wait_ms, max_ack_time_ms, max_msg_deliveries=10):
        self.connection = connection
        self.station_name = station_name.lower()
        self.consumer_name = consumer_name.lower()
        self.consumer_group = consumer_group.lower()
        self.pull_interval_ms = pull_interval_ms
        self.batch_size = batch_size
        self.batch_max_time_to_wait_ms = batch_max_time_to_wait_ms
        self.max_ack_time_ms = max_ack_time_ms
        self.max_msg_deliveries = max_msg_deliveries
        self.ping_consumer_invterval_ms = 30000

    def consume(self, callback):
        """Consume events.
        """
        self.t_consume = asyncio.create_task(self.__consume(callback))
        self.t_dlq = asyncio.create_task(self.__consume_dlq(callback))

    async def __consume(self, callback):
        self.psub = await self.connection.broker_connection.pull_subscribe(
            self.station_name + ".final", durable=self.consumer_group)
        while True:
            if self.connection.is_connection_active and self.pull_interval_ms:
                try:
                    memphis_messages = []
                    msgs = await self.psub.fetch(self.batch_size)
                    for msg in msgs:
                        memphis_messages.append(Message(msg))
                    await callback(memphis_messages, None)
                    await asyncio.sleep(self.pull_interval_ms/1000)
                except TimeoutError:
                    await callback([], Exception("Memphis: TimeoutError"))
                    continue
                except Exception as e:
                    if self.connection.is_connection_active:
                        raise Exception(e)
                    else:
                        return
            else:
                break

    async def __consume_dlq(self, callback):
        try:
            self.consumer_dlq = await self.connection.broker_manager.subscribe("$memphis_dlq_"+self.station_name+"_"+self.consumer_group, "$memphis_dlq_"+self.station_name+"_"+self.consumer_group)
            async for msg in self.consumer_dlq.messages:
                await callback([Message(msg)], None)
        except Exception as e:
            print("dls", e)
            await callback([], Exception(e))
            return

    async def __ping_consumer(self):
        x = await self.connection.broker_connection.consumer_info(self.station_name, durable=self.consumer_group)

    async def destroy(self):
        """Destroy the consumer.
        """
        self.pull_interval_ms = None
        try:
            destroyConsumerReq = {
                "name": self.consumer_name,
                "station_name":self.station_name
            }
            consumer_name = json.dumps(destroyConsumerReq, indent=2).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_consumer_destructions', consumer_name)
            error = res.data.decode('utf-8')
            if error != "" and not "not exist" in error:
                raise Exception(error)
        except Exception as e:
            raise Exception(e)


class Message:
    def __init__(self, message):
        self.message = message

    async def ack(self):
        """Ack a message is done processing.
        """
        try:
            await self.message.ack()
        except Exception as e:
            return

    def get_data(self):
        """Receive the message.
        """
        try:
            return self.message.data
        except:
            return
