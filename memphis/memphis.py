# Credit for The NATS.IO Authors
# Copyright 2021-2022 The Memphis Authors
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import random
import json

import nats as broker

from threading import Timer
import asyncio

from google.protobuf import descriptor_pb2, descriptor_pool, reflection
from google.protobuf.message_factory import MessageFactory
from google.protobuf.message import Message

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
        self.schema_updates_data = {}
        self.schema_updates_subs = {}
        self.producers_per_station = {}
        self.schema_tasks = {}
        self.proto_msgs = {}

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
            raise MemphisConnectError(str(e)) from e

    async def station(self, name, retention_type=retention_types.MAX_MESSAGE_AGE_SECONDS, retention_value=604800, storage_type=storage_types.DISK, replicas=1, dedup_enabled=False, dedup_window_ms=0):
        """Creates a station.
        Args:
            name (str): station name.
            retention_type (str, optional): retention type: message_age_sec/messages/bytes . Defaults to "message_age_sec".
            retention_value (int, optional): number which represents the retention based on the retention_type. Defaults to 604800.
            storage_type (str, optional): persistance storage for messages of the station: disk/memory. Defaults to "disk".
            replicas (int, optional):number of replicas for the messages of the data. Defaults to 1.
            dedup_enabled (bool, optional): whether to allow dedup mecanism, dedup happens based on message ID. Defaults to False.
            dedup_window_ms (int, optional): time frame in which dedup track messages. Defaults to 0.
        Returns:
            object: station
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")

            createStationReq = {
                "name": name,
                "retention_type": retention_type,
                "retention_value": retention_value,
                "storage_type": storage_type,
                "replicas": replicas,
                "dedup_enabled": dedup_enabled,
                "dedup_window_in_ms": dedup_window_ms
            }
            create_station_req_bytes = json.dumps(
                createStationReq, indent=2).encode('utf-8')
            err_msg = await self.broker_manager.request("$memphis_station_creations", create_station_req_bytes)
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)
            return Station(self, name)

        except Exception as e:
            if str(e).find('already exist') != -1:
                return Station(self, name.lower())
            else:
                raise MemphisError(str(e)) from e

    async def close(self):
        """Close Memphis connection.
        """
        try:
            if self.is_connection_active:
                await self.broker_manager.close()
                self.broker_manager = None
                self.connection_id = None
                self.is_connection_active = False
                keys_schema_updates_subs = self.schema_updates_subs.keys()
                for key in keys_schema_updates_subs:
                    sub = self.schema_updates_subs.get(key)
                    task = self.schema_tasks.get(key)
                    del self.schema_updates_data[key]
                    del self.schema_updates_subs[key]
                    del self.producers_per_station[key]
                    del self.schema_tasks[key]
                    task.cancel()
                    await sub.unsubscribe()
        except:
            return

    def __generateRandomSuffix(self, name: str) -> str:
        return name + "_" + random_bytes(8)

    def __generateConnectionID(self):
        return random_bytes(24)

    def __normalize_host(self, host):
        if (host.startswith("http://")):
            return host.split("http://")[1]
        elif (host.startswith("https://")):
            return host.split("https://")[1]
        else:
            return host

    async def producer(self, station_name, producer_name, generate_random_suffix=False):
        """Creates a producer.
        Args:
            station_name (str): station name to produce messages into.
            producer_name (str): name for the producer.
            generate_random_suffix (bool): false by default, if true concatenate a random suffix to producer's name
        Raises:
            Exception: _description_
            Exception: _description_
        Returns:
            _type_: _description_
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")

            if generate_random_suffix:
                producer_name = self.__generateRandomSuffix(producer_name)
            createProducerReq = {
                "name": producer_name,
                "station_name": station_name,
                "connection_id": self.connection_id,
                "producer_type": "application",
                "req_version": 1
            }
            create_producer_req_bytes = json.dumps(
                createProducerReq, indent=2).encode('utf-8')
            create_res = await self.broker_manager.request("$memphis_producer_creations", create_producer_req_bytes)
            create_res = create_res.data.decode("utf-8")
            create_res = json.loads(create_res)
            if create_res['error'] != "":
                raise MemphisError(create_res)

            station_name_internal = get_internal_name(station_name)
            await self.start_listen_for_schema_updates(station_name_internal, create_res['schema_update'])

            if self.schema_updates_data[station_name_internal] != {}:
                self.parse_descriptor(station_name_internal)
            return Producer(self, producer_name, station_name)

        except Exception as e:
            raise MemphisError(str(e)) from e

    async def get_msg_schema_updates(self, station_name_internal, iterable):
        async for msg in iterable:
            message = msg.data.decode("utf-8")
            message = json.loads(message)
            if message['init']['schema_name'] == "":
                data = {}
            else:
                data = message['init']
            self.schema_updates_data[station_name_internal] = data
            self.parse_descriptor(station_name_internal)

    def parse_descriptor(self, station_name):
        try:
            descriptor = self.schema_updates_data[station_name]['active_version']['descriptor']
            msg_struct_name = self.schema_updates_data[station_name]['active_version']['message_struct_name']
            desc_set = descriptor_pb2.FileDescriptorSet()
            descriptor_bytes = str.encode(descriptor)
            desc_set.ParseFromString(descriptor_bytes)
            pool = descriptor_pool.DescriptorPool()
            pool.Add(desc_set.file[0])
            pkg_name = desc_set.file[0].package
            msg_name = msg_struct_name
            if pkg_name != "":
                msg_name = desc_set.file[0].package + "." + msg_struct_name
            proto_msg = MessageFactory(pool).GetPrototype(
                pool.FindMessageTypeByName(msg_name))
            proto = proto_msg()
            self.proto_msgs[station_name] = proto

        except Exception as e:
            raise MemphisError(str(e)) from e

    async def start_listen_for_schema_updates(self, station_name, schema_update_data):
        schema_updates_subject = "$memphis_schema_updates_" + station_name

        empty = schema_update_data['schema_name'] == ''
        if empty:
            self.schema_updates_data[station_name] = {}
        else:
            self.schema_updates_data[station_name] = schema_update_data

        schema_exists = self.schema_updates_subs.get(station_name)
        if schema_exists:
            self.producers_per_station[station_name] += 1
        else:
            sub = await self.broker_manager.subscribe(schema_updates_subject)
            self.producers_per_station[station_name] = 1
            self.schema_updates_subs[station_name] = sub
        task_exists = self.schema_tasks.get(station_name)
        if not task_exists:
            loop = asyncio.get_event_loop()
            task = loop.create_task(self.get_msg_schema_updates(
                station_name, self.schema_updates_subs[station_name].messages))
            self.schema_tasks[station_name] = task

    async def consumer(self, station_name, consumer_name, consumer_group="", pull_interval_ms=1000, batch_size=10, batch_max_time_to_wait_ms=5000, max_ack_time_ms=30000, max_msg_deliveries=10, generate_random_suffix=False):
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
            generate_random_suffix (bool): false by default, if true concatenate a random suffix to consumer's name

        Returns:
            object: consumer
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")

            if generate_random_suffix:
                consumer_name = self.__generateRandomSuffix(consumer_name)
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

            create_consumer_req_bytes = json.dumps(
                createConsumerReq, indent=2).encode('utf-8')
            err_msg = await self.broker_manager.request("$memphis_consumer_creations", create_consumer_req_bytes)
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)

            return Consumer(self, station_name, consumer_name, cg, pull_interval_ms, batch_size, batch_max_time_to_wait_ms, max_ack_time_ms, max_msg_deliveries)

        except Exception as e:
            raise MemphisError(str(e)) from e


class Headers:
    def __init__(self):
        self.headers = {}

    def add(self, key, value):
        """Add a header.
        Args:
            key (string): header key.
            value (string): header value.
        Raises:
            Exception: _description_
        """
        if not key.startswith("$memphis"):
            self.headers[key] = value
        else:
            raise MemphisHeaderError("Keys in headers should not start with $memphis")


class Station:
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name.lower()

    async def destroy(self):
        """Destroy the station.
        """
        try:
            nameReq = {
                "station_name": self.name
            }
            station_name = json.dumps(nameReq, indent=2).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_station_destructions', station_name)
            error = res.data.decode('utf-8')
            if error != "" and not "not exist" in error:
                raise MemphisError(error)

            station_name_internal = get_internal_name(self.name)
            sub = self.connection.schema_updates_subs.get(
                station_name_internal)
            task = self.connection.schema_tasks.get(station_name_internal)
            del self.connection.schema_updates_data[station_name_internal]
            del self.connection.schema_updates_subs[station_name_internal]
            del self.connection.producers_per_station[station_name_internal]
            del self.connection.schema_tasks[station_name_internal]
            task.cancel()
            await sub.unsubscribe()

        except Exception as e:
            raise MemphisError(str(e)) from e


def get_internal_name(name: str) -> str:
    return name.replace(".", "#")


class Producer:
    def __init__(self, connection, producer_name, station_name):
        self.connection = connection
        self.producer_name = producer_name
        self.station_name = station_name
        self.internal_station_name = get_internal_name(self.station_name)

    def validate(self, message):
        proto_msg = self.connection.proto_msgs[self.internal_station_name]
        try:
            if isinstance(message, bytearray):
                proto_msg.ParseFromString(bytes(message))
                proto_msg.SerializeToString()
                return message
            elif hasattr(message, "SerializeToString"):
                string_msg = message.SerializeToString()
                proto_msg.ParseFromString(string_msg)
                proto_msg.SerializeToString()
                return string_msg

            else:
                raise MemphisSchemaError("Unsupported message type")

        except Exception as e:
            raise MemphisSchemaError("Schema validation has failed: " + str(e))

    async def produce(self, message, ack_wait_sec=15, headers={}, async_produce=False):
        """Produces a message into a station.
        Args:
            message (bytes): message to send into the station (bytes array / protobuf class -in case your station is schema validated).
            ack_wait_sec (int, optional): max time in seconds to wait for an ack from memphis. Defaults to 15.
            headers (dict, optional): Message headers, defaults to {}.
            async_produce (boolean, optional): produce operation won't wait for broker acknowledgement
        Raises:
            Exception: _description_
            Exception: _description_
        """
        try:
            if self.connection.schema_updates_data[self.internal_station_name] != {}:
                message = self.validate(message)
            elif not isinstance(message, bytearray):
                raise MemphisSchemaError("Unsupported message type")

            memphis_headers = {
                "$memphis_producedBy": self.producer_name,
                "$memphis_connectionId": self.connection.connection_id}
            if headers != {}:
                headers = headers.headers
                headers.update(memphis_headers)
            else:
                headers = memphis_headers

            if async_produce:
                self.connection.broker_connection.publish(
                    self.internal_station_name + ".final", message, timeout=ack_wait_sec, headers=headers)
            else:
                await self.connection.broker_connection.publish(self.internal_station_name + ".final", message, timeout=ack_wait_sec, headers=headers)
        except Exception as e:
            if hasattr(e, 'status_code') and e.status_code == '503':
                raise MemphisError(
                    "Produce operation has failed, please check whether Station/Producer are still exist")
            else:
                raise MemphisError(str(e)) from e

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

            station_name_internal = get_internal_name(self.station_name)
            producer_number = self.connection.producers_per_station.get(
                station_name_internal) - 1
            self.connection.producers_per_station[station_name_internal] = producer_number

            if producer_number == 0:
                sub = self.connection.schema_updates_subs.get(
                    station_name_internal)
                task = self.connection.schema_tasks.get(station_name_internal)
                del self.connection.schema_updates_data[station_name_internal]
                del self.connection.schema_updates_subs[station_name_internal]
                del self.connection.schema_tasks[station_name_internal]
                task.cancel()
                await sub.unsubscribe()

        except Exception as e:
            raise Exception(e)


async def default_error_handler(e):
    await print("ping exception raised", e)


class Consumer:
    def __init__(self, connection, station_name, consumer_name, consumer_group, pull_interval_ms, batch_size, batch_max_time_to_wait_ms, max_ack_time_ms, max_msg_deliveries=10, error_callback=None):
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
        if error_callback is None:
            error_callback = default_error_handler
        self.t_ping = asyncio.create_task(self.__ping_consumer(error_callback))

    def consume(self, callback):
        """Consume events.
        """
        self.t_consume = asyncio.create_task(self.__consume(callback))
        self.t_dlq = asyncio.create_task(self.__consume_dlq(callback))

    async def __consume(self, callback):
        subject = get_internal_name(self.station_name)
        consumer_group = get_internal_name(self.consumer_group)
        self.psub = await self.connection.broker_connection.pull_subscribe(
            subject + ".final", durable=consumer_group)
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
                    await callback([], MemphisError("Memphis: TimeoutError"))
                    continue
                except Exception as e:
                    if self.connection.is_connection_active:
                        raise MemphisError(str(e)) from e
                    else:
                        return
            else:
                break

    async def __consume_dlq(self, callback):
        subject = get_internal_name(self.station_name)
        consumer_group = get_internal_name(self.consumer_group)
        try:
            subscription_name = "$memphis_dlq_"+subject+"_"+consumer_group
            self.consumer_dlq = await self.connection.broker_manager.subscribe(subscription_name, subscription_name)
            async for msg in self.consumer_dlq.messages:
                await callback([Message(msg)], None)
        except Exception as e:
            print("dls", e)
            await callback([], MemphisError(str(e)))
            return

    async def __ping_consumer(self, callback):
        while True:
            try:
                await asyncio.sleep(self.ping_consumer_invterval_ms/1000)
                await self.connection.broker_connection.consumer_info(self.station_name, self.consumer_name)

            except Exception as e:
                await callback(e)

    async def destroy(self):
        """Destroy the consumer.
        """
        self.t_consume.cancel()
        self.t_dlq.cancel()
        self.t_ping.cancel()
        self.pull_interval_ms = None
        try:
            destroyConsumerReq = {
                "name": self.consumer_name,
                "station_name": self.station_name
            }
            consumer_name = json.dumps(
                destroyConsumerReq, indent=2).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_consumer_destructions', consumer_name)
            error = res.data.decode('utf-8')
            if error != "" and not "not exist" in error:
                raise MemphisError(error)
        except Exception as e:
            raise MemphisError(str(e)) from e


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

    def get_headers(self):
        """Receive the headers.
        """
        try:
            return self.message.headers
        except:
            return


def random_bytes(amount: int) -> str:
    lst = [random.choice('0123456789abcdef') for n in range(amount)]
    s = "".join(lst)
    return s


class MemphisError(Exception):
    def __init__(self, message):
        message = message.replace("nats", "memphis")
        message = message.replace("NATS", "memphis")
        message = message.replace("Nats", "memphis")
        message = message.replace("NatsError", "MemphisError")

        self.message = message
        super().__init__("memphis: " + self.message)


class MemphisConnectError(MemphisError):
    pass


class MemphisSchemaError(MemphisError):
    pass


class MemphisHeaderError(MemphisError):
    pass
