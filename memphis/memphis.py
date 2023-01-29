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
import ssl
import time
from graphql import build_schema as build_graphql_schema, parse as parse_graphql, validate as validate_graphql
import graphql

import nats as broker

from threading import Timer
import asyncio

from jsonschema import validate
from google.protobuf import descriptor_pb2, descriptor_pool
from google.protobuf.message_factory import MessageFactory
from google.protobuf.message import Message

import memphis.retention_types as retention_types
import memphis.storage_types as storage_types

schemaVFailAlertType = 'schema_validation_fail_alert'


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
        self.graphql_schemas = {}
        self.json_schemas = {}
        self.cluster_configurations = {}
        self.station_schemaverse_to_dls = {}
        self.update_configurations_sub = {}
        self.configuration_tasks = {}

    async def get_msgs_update_configurations(self, iterable):
        try:
            async for msg in iterable:
                message = msg.data.decode("utf-8")
                data = json.loads(message)
                if data['type'] == 'send_notification':
                    self.cluster_configurations[data['type']] = data['update']
                elif data['type'] == 'schemaverse_to_dls':
                    self.station_schemaverse_to_dls[data['station_name']
                                                    ] = data['update']
        except Exception as err:
            raise MemphisError(err)

    async def configurations_listener(self):
        try:
            sub = await self.broker_manager.subscribe("$memphis_sdk_configurations_updates")
            self.update_configurations_sub = sub
            loop = asyncio.get_event_loop()
            task = loop.create_task(self.get_msgs_update_configurations(
                self.update_configurations_sub.messages))
            self.configuration_tasks = task
        except Exception as err:
            raise MemphisError(err)

    async def connect(self, host, username, connection_token, port=6666, reconnect=True, max_reconnect=10, reconnect_interval_ms=1500, timeout_ms=15000, cert_file='', key_file='', ca_file=''):
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
            key_file (string): path to tls key file.
            cert_file (string): path to tls cert file.
            ca_file (string): path to tls ca file.
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
            if (cert_file != '' or key_file != '' or ca_file != ''):
                if cert_file == '':
                    raise MemphisConnectError("Must provide a TLS cert file")
                if key_file == '':
                    raise MemphisConnectError("Must provide a TLS key file")
                if ca_file == '':
                    raise MemphisConnectError("Must provide a TLS ca file")
                ssl_ctx = ssl.create_default_context(
                    purpose=ssl.Purpose.SERVER_AUTH)
                ssl_ctx.load_verify_locations(ca_file)
                ssl_ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
                self.broker_manager = await broker.connect(servers=self.host+":"+str(self.port),
                                                           allow_reconnect=self.reconnect,
                                                           reconnect_time_wait=self.reconnect_interval_ms/1000,
                                                           connect_timeout=self.timeout_ms/1000,
                                                           max_reconnect_attempts=self.max_reconnect,
                                                           token=self.connection_token,
                                                           name=self.connection_id + "::" + self.username,
                                                           tls=ssl_ctx,
                                                           tls_hostname=self.host)
            else:
                self.broker_manager = await broker.connect(servers=self.host+":"+str(self.port),
                                                           allow_reconnect=self.reconnect,
                                                           reconnect_time_wait=self.reconnect_interval_ms/1000,
                                                           connect_timeout=self.timeout_ms/1000,
                                                           max_reconnect_attempts=self.max_reconnect,
                                                           token=self.connection_token,
                                                           name=self.connection_id + "::" + self.username)

            await self.configurations_listener()
            self.broker_connection = self.broker_manager.jetstream()
            self.is_connection_active = True
        except Exception as e:
            raise MemphisConnectError(str(e)) from e

    async def send_notification(self, title, msg, failedMsg, type):
        msg = {
            "title": title,
            "msg": msg,
            "type": type,
            "code": failedMsg
        }
        msgToSend = json.dumps(msg).encode('utf-8')
        await self.broker_manager.publish("$memphis_notifications", msgToSend)

    async def station(self, name, retention_type=retention_types.MAX_MESSAGE_AGE_SECONDS, retention_value=604800, storage_type=storage_types.DISK, replicas=1, idempotency_window_ms=120000, schema_name="", send_poison_msg_to_dls=True, send_schema_failed_msg_to_dls=True):
        """Creates a station.
        Args:
            name (str): station name.
            retention_type (str, optional): retention type: message_age_sec/messages/bytes . Defaults to "message_age_sec".
            retention_value (int, optional): number which represents the retention based on the retention_type. Defaults to 604800.
            storage_type (str, optional): persistance storage for messages of the station: disk/memory. Defaults to "disk".
            replicas (int, optional):number of replicas for the messages of the data. Defaults to 1.
            idempotency_window_ms (int, optional): time frame in which idempotent messages will be tracked, happens based on message ID Defaults to 120000.
            schema_name (str): schema name.
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
                "idempotency_window_in_ms": idempotency_window_ms,
                "schema_name": schema_name,
                "dls_configuration": {
                    "poison": send_poison_msg_to_dls,
                    "Schemaverse": send_schema_failed_msg_to_dls
                },
                "username": self.username
            }
            create_station_req_bytes = json.dumps(
                createStationReq, indent=2).encode('utf-8')
            err_msg = await self.broker_manager.request("$memphis_station_creations", create_station_req_bytes, timeout=5)
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)
            return Station(self, name)

        except Exception as e:
            if str(e).find('already exist') != -1:
                return Station(self, name.lower())
            else:
                raise MemphisError(str(e)) from e

    async def attach_schema(self, name, stationName):
        """Attaches a schema to an existing station.
        Args:
            name (str): schema name.
            stationName (str): station name.
        Raises:
            Exception: _description_
        """
        try:
            if name == '' or stationName == '':
                raise MemphisError("name and station name can not be empty")
            msg = {
                "name": name,
                "station_name": stationName,
                "username": self.username
            }
            msgToSend = json.dumps(msg).encode('utf-8')
            err_msg = await self.broker_manager.request("$memphis_schema_attachments", msgToSend, timeout=5)
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)
        except Exception as e:
            raise MemphisError(str(e)) from e

    async def detach_schema(self, stationName):
        """Detaches a schema from station.
        Args:
            stationName (str): station name.
        Raises:
            Exception: _description_
        """
        try:
            if stationName == '':
                raise MemphisError("station name is missing")
            msg = {
                "station_name": stationName,
                "username": self.username
            }
            msgToSend = json.dumps(msg).encode('utf-8')
            err_msg = await self.broker_manager.request("$memphis_schema_detachments", msgToSend, timeout=5)
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)
        except Exception as e:
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
                self.configuration_tasks.cancel()
                for key in keys_schema_updates_subs:
                    sub = self.schema_updates_subs.get(key)
                    task = self.schema_tasks.get(key)
                    if key in self.schema_updates_data:
                        del self.schema_updates_data[key]
                    if key in self.schema_updates_subs:
                        del self.schema_updates_subs[key]
                    if key in self.producers_per_station:
                        del self.producers_per_station[key]
                    if key in self.schema_tasks:
                        del self.schema_tasks[key]
                    if task is not None:
                        task.cancel()
                    if sub is not None:
                        await sub.unsubscribe()
                if self.update_configurations_sub is not None:
                    await self.update_configurations_sub.unsubscribe()
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
                "req_version": 1,
                "username": self.username
            }
            create_producer_req_bytes = json.dumps(
                createProducerReq, indent=2).encode('utf-8')
            create_res = await self.broker_manager.request("$memphis_producer_creations", create_producer_req_bytes, timeout=5)
            create_res = create_res.data.decode("utf-8")
            create_res = json.loads(create_res)
            if create_res['error'] != "":
                raise MemphisError(create_res['error'])

            station_name_internal = get_internal_name(station_name)
            self.station_schemaverse_to_dls[station_name_internal] = create_res['schemaverse_to_dls']
            self.cluster_configurations['send_notification'] = create_res['send_notification']
            await self.start_listen_for_schema_updates(station_name_internal, create_res['schema_update'])

            if self.schema_updates_data[station_name_internal] != {}:
                if self.schema_updates_data[station_name_internal]['type'] == "protobuf":
                    self.parse_descriptor(station_name_internal)
                if self.schema_updates_data[station_name_internal]['type'] == "json":
                    schema = self.schema_updates_data[station_name_internal]['active_version']['schema_content']
                    self.json_schemas[station_name_internal] = json.loads(
                        schema)
                elif self.schema_updates_data[station_name_internal]['type'] == "graphql":
                    self.graphql_schemas[station_name_internal] = build_graphql_schema(
                        self.schema_updates_data[station_name_internal]['active_version']['schema_content'])
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

    async def consumer(self, station_name, consumer_name, consumer_group="", pull_interval_ms=1000, batch_size=10, batch_max_time_to_wait_ms=5000, max_ack_time_ms=30000, max_msg_deliveries=10, generate_random_suffix=False, start_consume_from_sequence=1, last_messages=-1):
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
            start_consume_from_sequence(int, optional): start consuming from a specific sequence. defaults to 1.
            last_messages: consume the last N messages, defaults to -1 (all messages in the station).
        Returns:
            object: consumer
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")

            if generate_random_suffix:
                consumer_name = self.__generateRandomSuffix(consumer_name)
            cg = consumer_name if not consumer_group else consumer_group

            if start_consume_from_sequence <= 0:
                raise MemphisError("start_consume_from_sequence has to be a positive number")

            if last_messages < -1:
                raise MemphisError("min value for last_messages is -1")

            if start_consume_from_sequence > 1 and last_messages > -1 :
                raise MemphisError("Consumer creation options can't contain both start_consume_from_sequence and last_messages")
            createConsumerReq = {
                'name': consumer_name,
                "station_name": station_name,
                "connection_id": self.connection_id,
                "consumer_type": 'application',
                "consumers_group": consumer_group,
                "max_ack_time_ms": max_ack_time_ms,
                "max_msg_deliveries": max_msg_deliveries,
                "start_consume_from_sequence": start_consume_from_sequence,
                "last_messages": last_messages,
                "req_version": 1,
                "username": self.username
            }

            create_consumer_req_bytes = json.dumps(
                createConsumerReq, indent=2).encode('utf-8')
            err_msg = await self.broker_manager.request("$memphis_consumer_creations", create_consumer_req_bytes, timeout=5)
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)

            return Consumer(self, station_name, consumer_name, cg, pull_interval_ms, batch_size, batch_max_time_to_wait_ms, max_ack_time_ms, max_msg_deliveries, start_consume_from_sequence=start_consume_from_sequence, last_messages=last_messages)

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
            raise MemphisHeaderError(
                "Keys in headers should not start with $memphis")


class Station:
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name.lower()

    async def destroy(self):
        """Destroy the station.
        """
        try:
            nameReq = {
                "station_name": self.name,
                "username": self.connection.username
            }
            station_name = json.dumps(nameReq, indent=2).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_station_destructions', station_name, timeout=5)
            error = res.data.decode('utf-8')
            if error != "" and not "not exist" in error:
                raise MemphisError(error)

            station_name_internal = get_internal_name(self.name)
            sub = self.connection.schema_updates_subs.get(
                station_name_internal)
            task = self.connection.schema_tasks.get(station_name_internal)
            if station_name_internal in self.connection.schema_updates_data:
                del self.connection.schema_updates_data[station_name_internal]
            if station_name_internal in self.connection.schema_updates_subs:
                del self.connection.schema_updates_subs[station_name_internal]
            if station_name_internal in self.connection.producers_per_station:
                del self.connection.producers_per_station[station_name_internal]
            if station_name_internal in self.connection.schema_tasks:
                del self.connection.schema_tasks[station_name_internal]
            if task is not None:
                task.cancel()
            if sub is not None:
                await sub.unsubscribe()

        except Exception as e:
            raise MemphisError(str(e)) from e


def get_internal_name(name: str) -> str:
    name = name.lower()
    return name.replace(".", "#")


class Producer:
    def __init__(self, connection, producer_name, station_name):
        self.connection = connection
        self.producer_name = producer_name.lower()
        self.station_name = station_name
        self.internal_station_name = get_internal_name(self.station_name)
        self.loop = asyncio.get_running_loop()

    async def validate_msg(self, message):
        if self.connection.schema_updates_data[self.internal_station_name] != {}:
            schema_type = self.connection.schema_updates_data[self.internal_station_name]['type']
            if schema_type == "protobuf":
                message = self.validate_protobuf(message)
                return message
            elif schema_type == "json":
                message = self.validate_json_schema(message)
                return message
            elif schema_type == "graphql":
                message = self.validate_graphql(message)
                return message
        elif not isinstance(message, bytearray) and not isinstance(message, dict) :
            raise MemphisSchemaError("Unsupported message type")
        else:
            if isinstance(message, dict):
                message = bytearray(json.dumps(message).encode('utf-8'))
            return message

    def validate_protobuf(self, message):
        proto_msg = self.connection.proto_msgs[self.internal_station_name]
        msgToSend = ""
        try:
            if isinstance(message, bytearray):
                msgToSend = bytes(message)
                try:
                    proto_msg.ParseFromString(msgToSend)
                    proto_msg.SerializeToString()
                    msgToSend = msgToSend.decode("utf-8")
                except Exception as e:
                    if 'parsing message' in str(e):
                        e = 'Invalid message format, expecting protobuf'
                    raise MemphisSchemaError(str(e))
                return message
            elif hasattr(message, "SerializeToString"):
                msgToSend = message.SerializeToString()
                proto_msg.ParseFromString(msgToSend)
                proto_msg.SerializeToString()
                return msgToSend

            else:
                raise MemphisSchemaError("Unsupported message type")

        except Exception as e:
            raise MemphisSchemaError("Schema validation has failed: " + str(e))

    def validate_json_schema(self, message):
        try:
            if isinstance(message, bytearray):
                try:
                    message_obj = json.loads(message)
                except Exception as e:
                    raise Exception("Expecting Json format: " + str(e))
            elif isinstance(message, dict):
                message_obj = message
                message = bytearray(json.dumps(message_obj).encode('utf-8'))
            else:
                raise Exception("Unsupported message type")

            validate(instance=message_obj,
                     schema=self.connection.json_schemas[self.internal_station_name])
            return message
        except Exception as e:
            raise MemphisSchemaError("Schema validation has failed: " + str(e))

    def validate_graphql(self, message):
        try:
            if isinstance(message, bytearray):
                msg = message.decode("utf-8")
                msg = parse_graphql(msg)
            elif isinstance(message, str):
                msg = parse_graphql(message)
                message = message.encode('utf-8')
            elif isinstance(message, graphql.language.ast.DocumentNode):
                msg = message
                message = str(msg.loc.source.body)
                message = message.encode('utf-8')
            else:
                raise MemphisError("Unsupported message type")
            validate_res = validate_graphql(
                schema=self.connection.graphql_schemas[self.internal_station_name], document_ast=msg)
            if len(validate_res) > 0:
                raise Exception(
                    "Schema validation has failed: " + str(validate_res))
            return message
        except Exception as e:
            if 'Syntax Error' in str(e):
                e = "Invalid message format, expected GraphQL"
            raise Exception("Schema validation has failed: " + str(e))

    def get_dls_msg_id(self, station_name, producer_name, unix_time):
        return station_name + '~' + producer_name + '~0~' + unix_time

    async def produce(self, message, ack_wait_sec=15, headers={}, async_produce=False, msg_id=None):
        """Produces a message into a station.
        Args:
            message (bytearray/dict): message to send into the station - bytearray/protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema)
            ack_wait_sec (int, optional): max time in seconds to wait for an ack from memphis. Defaults to 15.
            headers (dict, optional): Message headers, defaults to {}.
            async_produce (boolean, optional): produce operation won't wait for broker acknowledgement
            msg_id (string, optional): Attach msg-id header to the message in order to achieve idempotency
        Raises:
            Exception: _description_
            Exception: _description_
        """
        try:
            message = await self.validate_msg(message)

            memphis_headers = {
                "$memphis_producedBy": self.producer_name,
                "$memphis_connectionId": self.connection.connection_id}

            if msg_id != None:
                memphis_headers["msg-id"] = msg_id

            if headers != {}:
                headers = headers.headers
                headers.update(memphis_headers)
            else:
                headers = memphis_headers

            if async_produce:
                try:
                    self.loop.create_task(self.connection.broker_connection.publish(
                        self.internal_station_name + ".final", message, timeout=ack_wait_sec, headers=headers))
                    await asyncio.sleep(1)
                except Exception as e:
                    raise MemphisError(e)
            else:
                await self.connection.broker_connection.publish(self.internal_station_name + ".final", message, timeout=ack_wait_sec, headers=headers)
        except Exception as e:
            if hasattr(e, 'status_code') and e.status_code == '503':
                raise MemphisError(
                    "Produce operation has failed, please check whether Station/Producer are still exist")
            else:
                if ("Schema validation has failed" in str(e) or "Unsupported message type" in str(e)):
                    msgToSend = ""
                    if isinstance(message, bytearray):
                        msgToSend = str(message, 'utf-8')
                    elif hasattr(message, "SerializeToString"):
                        msgToSend = message.SerializeToString().decode("utf-8")
                    if self.connection.station_schemaverse_to_dls[self.internal_station_name]:
                        unix_time = int(time.time())
                        id = self.get_dls_msg_id(
                            self.internal_station_name, self.producer_name, str(unix_time))

                        memphis_headers = {
                            "$memphis_producedBy": self.producer_name,
                            "$memphis_connectionId": self.connection.connection_id}

                        if headers != {}:
                            headers = headers.headers
                            headers.update(memphis_headers)
                        else:
                            headers = memphis_headers

                        msgToSendEncoded = msgToSend.encode('utf-8')
                        msgHex = msgToSendEncoded.hex()
                        buf = {
                            "_id": id,
                            "station_name": self.internal_station_name,
                            "producer": {
                                "name": self.producer_name,
                                "connection_id": self.connection.connection_id
                            },
                            "creation_unix": unix_time,
                            "message": {
                                "data": msgHex,
                                "headers": headers,
                            }
                        }
                        buf = json.dumps(buf).encode('utf-8')
                        await self.connection.broker_connection.publish('$memphis-' + self.internal_station_name + '-dls.schema.' + id, buf)
                        if self.connection.cluster_configurations.get('send_notification'):
                            await self.connection.send_notification('Schema validation has failed', 'Station: ' + self.station_name + '\nProducer: ' + self.producer_name + '\nError:' + str(e), msgToSend, schemaVFailAlertType)
                raise MemphisError(str(e)) from e

    async def destroy(self):
        """Destroy the producer.
        """
        try:
            destroyProducerReq = {
                "name": self.producer_name,
                "station_name": self.station_name,
                "username": self.connection.username
            }

            producer_name = json.dumps(destroyProducerReq).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_producer_destructions', producer_name, timeout=5)
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
                if station_name_internal in self.connection.schema_updates_data:
                    del self.connection.schema_updates_data[station_name_internal]
                if station_name_internal in self.connection.schema_updates_subs:
                    del self.connection.schema_updates_subs[station_name_internal]
                if station_name_internal in self.connection.schema_tasks:
                    del self.connection.schema_tasks[station_name_internal]
                if task is not None:
                    task.cancel()
                if sub is not None:
                    await sub.unsubscribe()

        except Exception as e:
            raise Exception(e)


async def default_error_handler(e):
    await print("ping exception raised", e)


class Consumer:
    def __init__(self, connection, station_name, consumer_name, consumer_group, pull_interval_ms, batch_size, batch_max_time_to_wait_ms, max_ack_time_ms, max_msg_deliveries=10, error_callback=None, start_consume_from_sequence=1, last_messages=-1):
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
        self.start_consume_from_sequence = start_consume_from_sequence
        self.last_messages= last_messages
        self.context = {}

    def set_context(self, context):
        """Set a context (dict) that will be passed to each message handler call.
        """
        self.context = context

    def consume(self, callback):
        """Consume events.
        """
        self.t_consume = asyncio.create_task(self.__consume(callback))
        self.t_dls = asyncio.create_task(self.__consume_dls(callback))

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
                        memphis_messages.append(
                            Message(msg, self.connection, self.consumer_group))
                    await callback(memphis_messages, None, self.context)
                    await asyncio.sleep(self.pull_interval_ms/1000)
                except asyncio.TimeoutError:
                    await callback([], MemphisError("Memphis: TimeoutError"), self.context)
                    continue
                except Exception as e:
                    if self.connection.is_connection_active:
                        raise MemphisError(str(e)) from e
                    else:
                        return
            else:
                break

    async def __consume_dls(self, callback):
        subject = get_internal_name(self.station_name)
        consumer_group = get_internal_name(self.consumer_group)
        try:
            subscription_name = "$memphis_dls_"+subject+"_"+consumer_group
            self.consumer_dls = await self.connection.broker_manager.subscribe(subscription_name, subscription_name)
            async for msg in self.consumer_dls.messages:
                await callback([Message(msg, self.connection, self.consumer_group)], None, self.context)
        except Exception as e:
            print("dls", e)
            await callback([], MemphisError(str(e)))
            return

    async def __ping_consumer(self, callback):
        while True:
            try:
                await asyncio.sleep(self.ping_consumer_invterval_ms/1000)
                consumer_group = get_internal_name(self.consumer_group)
                await self.connection.broker_connection.consumer_info(self.station_name, consumer_group, timeout=30)

            except Exception as e:
                await callback(e)

    async def destroy(self):
        """Destroy the consumer.
        """
        if self.t_consume is not None:
            self.t_consume.cancel()
        if self.t_dls is not None:
            self.t_dls.cancel()
        if self.t_ping is not None:
            self.t_ping.cancel()
        self.pull_interval_ms = None
        try:
            destroyConsumerReq = {
                "name": self.consumer_name,
                "station_name": self.station_name,
                "username": self.connection.username
            }
            consumer_name = json.dumps(
                destroyConsumerReq, indent=2).encode('utf-8')
            res = await self.connection.broker_manager.request('$memphis_consumer_destructions', consumer_name, timeout=5)
            error = res.data.decode('utf-8')
            if error != "" and not "not exist" in error:
                raise MemphisError(error)
        except Exception as e:
            raise MemphisError(str(e)) from e


class Message:
    def __init__(self, message, connection, cg_name):
        self.message = message
        self.connection = connection
        self.cg_name = cg_name

    async def ack(self):
        """Ack a message is done processing.
        """
        try:
            await self.message.ack()
        except Exception as e:
            if ("$memphis_pm_id" in self.message.headers & "$memphis_pm_sequence" in self.message.headers):
                try:
                    msg = {
                        "id": self.message.headers["$memphis_pm_id"],
                        "sequence": self.message.headers["$memphis_pm_sequence"],
                    }
                    msgToAck = json.dumps(msg).encode('utf-8')
                    await self.connection.broker_manager.publish("$memphis_pm_acks", msgToAck)
                except Exception as er:
                    raise MemphisConnectError(str(er)) from er
            else:
                raise MemphisConnectError(str(e)) from e
            return

    def get_data(self):
        """Receive the message.
        """
        try:
            return bytearray(self.message.data)
        except:
            return

    def get_headers(self):
        """Receive the headers.
        """
        try:
            return self.message.headers
        except:
            return

    def get_sequence_number(self):
        """Get message sequence number.
        """
        try:
            return self.message.metadata.sequence.stream
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
        if message.startswith("memphis:"):
            super().__init__(self.message)
        else:
            super().__init__("memphis: " + self.message)


class MemphisConnectError(MemphisError):
    pass


class MemphisSchemaError(MemphisError):
    pass


class MemphisHeaderError(MemphisError):
    pass
