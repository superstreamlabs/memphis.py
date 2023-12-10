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
from __future__ import annotations

import asyncio
import copy
import json
import ssl
from typing import Iterable, Union, List
import uuid
import base64
import re
import warnings

import nats as broker
from google.protobuf import descriptor_pb2, descriptor_pool
from google.protobuf.message_factory import MessageFactory
from graphql import build_schema as build_graphql_schema
from memphis.consumer import Consumer
from memphis.exceptions import MemphisConnectError, MemphisError
from memphis.headers import Headers
from memphis.producer import Producer
from memphis.station import Station
from memphis.types import Retention, Storage
from memphis.utils import get_internal_name, random_bytes
from memphis.partition_generator import PartitionGenerator

app_id = str(uuid.uuid4())
# pylint: disable=too-many-lines
class Memphis:
    MAX_BATCH_SIZE = 5000
    MEMPHIS_GLOBAL_ACCOUNT_NAME = "$memphis"
    SEED = 31

    def __init__(self):
        self.is_connection_active = False
        self.schema_updates_data = {}
        self.partition_producers_updates_data = {}
        self.partition_consumers_updates_data = {}
        self.functions_updates_data = {}
        self.functions_updates_subs = {}
        self.functions_tasks = {}
        self.functions_clients_per_station = {}
        self.schema_updates_subs = {}
        self.clients_per_station = {}
        self.schema_tasks = {}
        self.proto_msgs = {}
        self.graphql_schemas = {}
        self.json_schemas = {}
        self.avro_schemas = {}
        self.cluster_configurations = {}
        self.station_schemaverse_to_dls = {}
        self.update_configurations_sub = {}
        self.configuration_tasks = {}
        self.producers_map = {}
        self.consumers_map = {}

    async def get_msgs_sdk_clients_updates(self, iterable: Iterable):
        try:
            async for msg in iterable:
                message = msg.data.decode("utf-8")
                data = json.loads(message)
                if data["type"] == "send_notification":
                    self.cluster_configurations[data["type"]] = data["update"]
                elif data["type"] == "schemaverse_to_dls":
                    self.station_schemaverse_to_dls[data["station_name"]] = data[
                        "update"
                    ]
                elif data["type"] == "remove_station":
                    self.unset_cached_producer_station(data['station_name'])
                    self.unset_cached_consumer_station(data['station_name'])
        except Exception as err:
            raise MemphisError(err)

    async def sdk_client_updates_listener(self):
        try:
            sub = await self.broker_manager.subscribe(
                "$memphis_sdk_clients_updates"
            )
            self.update_configurations_sub = sub
            loop = asyncio.get_event_loop()
            task = loop.create_task(
                self.get_msgs_sdk_clients_updates(
                    self.update_configurations_sub.messages
                )
            )
            self.configuration_tasks = task
        except Exception as err:
            raise MemphisError(err)

    async def get_broker_manager_connection(self, connection_opts):
        if "user" in connection_opts:
            async def ping_error_cb(e):
                if "authorization violation" not in (str(e)).lower():
                    print(MemphisError(str(e)))
            async def error_cb(e):
                return
            ping_connection_opts = copy.deepcopy(connection_opts)
            ping_connection_opts["allow_reconnect"] = False
            ping_connection_opts["error_cb"] = ping_error_cb

            # Newer versions of Memphis take a user of the form
            # "{username}${account_id}".  For older versions, the user was
            # simply the username.  The user is created using the new style
            # in Memphis.connect(). If connecting with the new-style user
            # fails, try the old style.
            try:
                conn = await broker.connect(**ping_connection_opts)
                await conn.close()
            except Exception as e:
                if "authorization violation" in str(e).lower():
                    try:
                        if "localhost" in connection_opts['servers']: # for handling bad quality networks like port fwd
                            await asyncio.sleep(1)
                        ping_connection_opts["user"] = self.username
                        ping_connection_opts["error_cb"] = error_cb
                        conn = await broker.connect(**ping_connection_opts)
                        await conn.close()
                        connection_opts["user"] = self.username
                    except Exception as e_1:
                        raise e_1
                else:
                    raise e

        if "localhost" in connection_opts['servers']:
            await asyncio.sleep(1) # for handling bad quality networks like port fwd
        return await broker.connect(**connection_opts)

    async def connect(
        self,
        host: str,
        username: str,
        account_id: int = 1,
        connection_token: str = "",
        password: str = "",
        port: int = 6666,
        reconnect: bool = True,
        max_reconnect: int = -1,
        reconnect_interval_ms: int = 1500,
        timeout_ms: int = 2000,
        cert_file: str = "",
        key_file: str = "",
        ca_file: str = "",
    ):
        """Creates connection with Memphis.
        Args:
            host (str): memphis host.
            username (str): user of type root/application.
            account_id (int): You can find it on the profile page in the Memphis UI. This field should be sent only on the cloud version of Memphis, otherwise it will be ignored
            connection_token (str): connection token.
            password (str): depends on how Memphis deployed - default is connection token-based authentication.
            port (int, optional): port. Defaults to 6666.
            reconnect (bool, optional): whether to do reconnect while connection is lost. Defaults to True.
            max_reconnect (int, optional): The maximum number of reconnection attempts. The default value is -1 which means reconnect indefinitely.
            reconnect_interval_ms (int, optional): Interval in milliseconds between reconnect attempts. Defaults to 1500.
            timeout_ms (int, optional): connection timeout in milliseconds. Defaults to 15000.
            key_file (string): path to tls key file.
            cert_file (string): path to tls cert file.
            ca_file (string): path to tls ca file.
        """
        self.host = self.__normalize_host(host)
        self.username = username
        self.account_id = account_id
        self.connection_token = connection_token
        self.password = password
        self.port = port
        self.reconnect = reconnect
        self.max_reconnect = 9 if max_reconnect > 9 else max_reconnect
        self.reconnect_interval_ms = reconnect_interval_ms
        self.timeout_ms = timeout_ms
        self.connection_id = str(uuid.uuid4())
        try:
            if self.connection_token != "" and self.password != "":
                raise MemphisConnectError(
                    "You have to connect with one of the following methods: connection token / password")
            if self.connection_token == "" and self.password == "":
                raise MemphisConnectError(
                    "You have to connect with one of the following methods: connection token / password")
            self.broker_manager = None
            async def closed_callback():
                if self.broker_manager is not None and self.broker_manager.last_error is not None:
                    print(MemphisError(str(self.broker_manager.last_error)))

            connection_opts = {
                "servers": self.host + ":" + str(self.port),
                "allow_reconnect": self.reconnect,
                "reconnect_time_wait": self.reconnect_interval_ms / 1000,
                "connect_timeout": self.timeout_ms / 1000,
                "max_reconnect_attempts": self.max_reconnect,
                "name": self.connection_id + "::" + self.username,
                "closed_cb": closed_callback,
            }
            if cert_file != "" or key_file != "" or ca_file != "":
                if cert_file == "":
                    raise MemphisConnectError("Must provide a TLS cert file")
                if key_file == "":
                    raise MemphisConnectError("Must provide a TLS key file")
                if ca_file == "":
                    raise MemphisConnectError("Must provide a TLS ca file")
                ssl_ctx = ssl.create_default_context(
                    purpose=ssl.Purpose.SERVER_AUTH)
                ssl_ctx.load_verify_locations(ca_file)
                ssl_ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
                connection_opts["tls"] = ssl_ctx
                connection_opts["tls_hostname"] = self.host
            if self.connection_token != "":
                connection_opts["token"] = self.connection_token
            else:
                connection_opts["user"] = self.username + \
                    "$" + str(self.account_id)
                connection_opts["password"] = self.password

            self.broker_manager = await self.get_broker_manager_connection(connection_opts)
            await self.sdk_client_updates_listener()
            self.broker_connection = self.broker_manager.jetstream()
            self.is_connection_active = True
        except Exception as e:
            raise MemphisError(str(e))

    async def send_notification(self, title, msg, failed_msg, not_type):
        msg = {"title": title, "msg": msg, "type": not_type, "code": failed_msg}
        msg_to_send = json.dumps(msg).encode("utf-8")
        await self.broker_manager.publish("$memphis_notifications", msg_to_send)

    async def station(
        self,
        name: str,
        retention_type: Retention = Retention.MAX_MESSAGE_AGE_SECONDS,
        retention_value: int = 3600,
        storage_type: Storage = Storage.DISK,
        replicas: int = 1,
        idempotency_window_ms: int = 120000,
        schema_name: str = "",
        send_poison_msg_to_dls: bool = True,
        send_schema_failed_msg_to_dls: bool = True,
        tiered_storage_enabled: bool = False,
        partitions_number: int = 1,
        dls_station: str = "",
        timeout_retries=5,
    ):
        """Creates a station.
        Args:
            name (str): station name.
            retention_type (Retention, optional): retention type: MESSAGE_AGE_SEC/MESSAGES/BYTES/ACK_BASED . Defaults to "MESSAGE_AGE_SEC".
            retention_value (int, optional): number which represents the retention based on the retention_type. Defaults to 604800.
            storage_type (Storage, optional): persistance storage for messages of the station: disk/memory. Defaults to "disk".
            replicas (int, optional):number of replicas for the messages of the data. Defaults to 1.
            idempotency_window_ms (int, optional): time frame in which idempotent messages will be tracked, happens based on message ID Defaults to 120000.
            schema_name (str): schema name.
            send_poison_msg_to_dls (bool): whether unacked(poison) messages (reached the max deliveries) should be sent into the DLS. Defaults to True.
            send_schema_failed_msg_to_dls (bool): whether schema violation messages should be sent into the DLS. Defaults to True.
            tiered_storage_enabled (bool): if true + tiered storage configured - messages hit the retention will be moved into tier 2 storage. Defaults to False.
            partitions_number (int): number of partitions for the station. Defaults to 1.
            dls_station (str): If selected DLS events will be sent to selected station as well. Defaults to "".
        Returns:
            object: station
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")
            if partitions_number == 0:
                partitions_number = 1

            create_station_req = {
                "name": name,
                "retention_type": retention_type.value,
                "retention_value": retention_value,
                "storage_type": storage_type.value,
                "replicas": replicas,
                "idempotency_window_in_ms": idempotency_window_ms,
                "schema_name": schema_name,
                "dls_configuration": {
                    "poison": send_poison_msg_to_dls,
                    "Schemaverse": send_schema_failed_msg_to_dls,
                },
                "username": self.username,
                "tiered_storage_enabled": tiered_storage_enabled,
                "partitions_number" : partitions_number,
                "dls_station": dls_station
            }
            create_station_req_bytes = json.dumps(create_station_req, indent=2).encode(
                "utf-8"
            )
            err_msg = await self._request(
                "$memphis_station_creations", create_station_req_bytes, 20, timeout_retries
            )
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)
            return Station(self, name)

        except Exception as e:
            if str(e).find("already exist") != -1:
                return Station(self, name.lower())
            raise MemphisError(str(e)) from e

    async def attach_schema(self, name, station_name):
        """Deprecated - Use enforce_schema instead
        Args:
            name (str): schema name.
            station_name (str): station name.
        Raises:
            Exception: _description_
        """
        await self.enforce_schema(name, station_name)

    async def enforce_schema(self, name, station_name, timeout_retries=5):
        """Enforce a schema on an existing station.
        Args:
            name (str): schema name.
            station_name (str): station name.
        Raises:
            Exception: _description_
        """
        try:
            if name == "" or station_name == "":
                raise MemphisError("name and station name can not be empty")
            msg = {"name": name, "station_name": station_name,
                   "username": self.username}
            msg_to_send = json.dumps(msg).encode("utf-8")
            err_msg = await self._request(
                "$memphis_schema_attachments", msg_to_send, 20, timeout_retries
            )
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)
        except Exception as e:
            raise MemphisError(str(e)) from e

    async def detach_schema(self, station_name, timeout_retries=5):
        """Detaches a schema from station.
        Args:
            station_name (str): station name.
        Raises:
            Exception: _description_
        """
        try:
            if station_name == "":
                raise MemphisError("station name is missing")
            msg = {"station_name": station_name, "username": self.username}
            msg_to_send = json.dumps(msg).encode("utf-8")
            err_msg = await self._request(
                "$memphis_schema_detachments", msg_to_send, 20, timeout_retries
            )
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)
        except Exception as e:
            raise MemphisError(str(e)) from e

    async def close(self):
        """Close Memphis connection."""
        try:
            if self.is_connection_active:
                await self.broker_manager.close()
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
                    if key in self.clients_per_station:
                        del self.clients_per_station[key]
                    if key in self.schema_tasks:
                        del self.schema_tasks[key]
                    if task is not None:
                        task.cancel()
                    if sub is not None:
                        await sub.unsubscribe()
                if self.update_configurations_sub is not None:
                    await self.update_configurations_sub.unsubscribe()
                self.producers_map.clear()
                for consumer in self.consumers_map:
                    consumer.dls_messages.clear()
                self.consumers_map.clear()
        except Exception:
            return

    def __generate_random_suffix(self, name: str) -> str:
        return name + "_" + random_bytes(8)

    def __normalize_host(self, host):
        if host.startswith("http://"):
            return host.split("http://")[1]
        if host.startswith("https://"):
            return host.split("https://")[1]
        return host

    async def _request(self, subject, payload, timeout, timeout_retries=5):
        try:
            res = await self.broker_manager.request(subject, payload, timeout=timeout)
            return res
        except Exception as e:
            if 'timeout' not in str(e).lower() or timeout_retries <= 0:
                raise MemphisError(str(e)) from e
            return await self._request(subject, payload, timeout=timeout, timeout_retries=timeout_retries-1)

    async def producer(
        self,
        station_name: Union[str, List[str]],
        producer_name: str,
        generate_random_suffix: bool = False,
        timeout_retries=5,
    ):
        """Creates a producer.
        Args:
            station_name (Union[str, List[str]]): station name to produce messages into.
            producer_name (str): name for the producer.
            generate_random_suffix (bool): Deprecated: will be stopped to be supported after November 1'st, 2023. false by default, if true concatenate a random suffix to producer's name
        Raises:
            Exception: _description_
        Returns:
            _type_: _description_
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")
            if not isinstance(station_name, str) and not isinstance(station_name, list):
                raise MemphisError("station_name should be either string or list of strings")
            real_name = producer_name.lower()
            if generate_random_suffix:
                warnings.warn("Deprecation warning: generate_random_suffix will be stopped to be supported after November 1'st, 2023.")
                producer_name = self.__generate_random_suffix(producer_name)
            if isinstance(station_name, str):
                return await self._single_station_producer(station_name, producer_name, real_name, timeout_retries)
            else:
                return await self._multi_station_producer(station_name, producer_name, real_name)
        except Exception as e:
            raise MemphisError(str(e)) from e

    async def _single_station_producer(
        self,
        station_name: str,
        producer_name: str,
        real_name: str,
        timeout_retries: int,
    ):
        try:
            internal_station_name = get_internal_name(station_name)
            map_key = internal_station_name + "_" + producer_name.lower()
            producer = None
            if map_key in self.producers_map:
                return self.producers_map[map_key]

            create_producer_req = {
                "name": producer_name,
                "station_name": station_name,
                "connection_id": self.connection_id,
                "producer_type": "application",
                "req_version": 3,
                "username": self.username,
                "app_id": app_id,
            }
            create_producer_req_bytes = json.dumps(create_producer_req, indent=2).encode(
                "utf-8"
            )
            create_res = await self._request(
                "$memphis_producer_creations", create_producer_req_bytes, 20, timeout_retries
            )
            create_res = create_res.data.decode("utf-8")
            create_res = json.loads(create_res)
            if create_res["error"] != "":
                raise MemphisError(create_res["error"])

            if "partitions_update" in create_res:
                if create_res["partitions_update"]["partitions_list"] is not None:
                    self.partition_producers_updates_data[internal_station_name] = create_res[
                        "partitions_update"
                    ]

            self.station_schemaverse_to_dls[internal_station_name] = create_res[
                "schemaverse_to_dls"
            ]
            self.cluster_configurations["send_notification"] = create_res[
                "send_notification"
            ]
            await self.start_listen_for_schema_updates(
                internal_station_name, create_res["schema_update"]
            )

            self.update_schema_data(station_name)

            if "station_version" in create_res:
                if create_res["station_version"] >= 2:
                    await self.start_listen_for_functions_updates(internal_station_name, create_res["station_partitions_first_functions"])

            producer = Producer(self, producer_name, station_name, real_name)
            map_key = internal_station_name + "_" + real_name
            self.producers_map[map_key] = producer
            return producer

        except Exception as e:
            raise MemphisError(str(e)) from e


    async def _multi_station_producer(
        self,
        station_names: List[str],
        producer_name: str,
        real_name: str
    ):
        return Producer(self, producer_name, station_names, real_name)

    def update_schema_data(self, station_name):
        internal_station_name = get_internal_name(station_name)
        if self.schema_updates_data[internal_station_name] != {}:
            if (
                self.schema_updates_data[internal_station_name]["type"]
                == "protobuf"
            ):
                self.parse_descriptor(internal_station_name)
            if self.schema_updates_data[internal_station_name]["type"] == "json":
                schema = self.schema_updates_data[internal_station_name][
                    "active_version"
                ]["schema_content"]
                self.json_schemas[internal_station_name] = json.loads(
                    schema)
            elif (
                self.schema_updates_data[internal_station_name]["type"] == "graphql"
            ):
                self.graphql_schemas[internal_station_name] = build_graphql_schema(
                    self.schema_updates_data[internal_station_name][
                        "active_version"
                    ]["schema_content"]
                )
            elif (
                self.schema_updates_data[internal_station_name]["type"] == "avro"
            ):
                schema = self.schema_updates_data[internal_station_name][
                    "active_version"
                ]["schema_content"]
                self.avro_schemas[internal_station_name] = json.loads(
                    schema)

    async def get_msg_schema_updates(self, internal_station_name, iterable):
        async for msg in iterable:
            message = msg.data.decode("utf-8")
            message = json.loads(message)
            if message["init"]["schema_name"] == "":
                data = {}
            else:
                data = message["init"]
            self.schema_updates_data[internal_station_name] = data
            if message["init"]["type"] == "protobuf":
                self.parse_descriptor(internal_station_name)

    def parse_descriptor(self, station_name):
        try:
            descriptor = self.schema_updates_data[station_name]["active_version"][
                "descriptor"
            ]
            msg_struct_name = self.schema_updates_data[station_name]["active_version"][
                "message_struct_name"
            ]
            desc_set = descriptor_pb2.FileDescriptorSet()
            descriptor_bytes = base64.b64decode(descriptor)
            desc_set.ParseFromString(descriptor_bytes)
            pool = descriptor_pool.DescriptorPool()
            pool.Add(desc_set.file[0])
            pkg_name = desc_set.file[0].package
            msg_name = msg_struct_name
            if pkg_name != "":
                msg_name = desc_set.file[0].package + "." + msg_struct_name
            proto_msg = MessageFactory(pool).GetPrototype(
                pool.FindMessageTypeByName(msg_name)
            )
            proto = proto_msg()
            self.proto_msgs[station_name] = proto

        except Exception as e:
            raise MemphisError(str(e)) from e

    async def start_listen_for_functions_updates(self, station_name, first_functions):
        #first_functions should contain the dict of the first function of each partition key: partition number, value: first function id

        if station_name in self.functions_updates_subs:
            self.functions_clients_per_station[station_name] += 1
            return
        else:
            self.functions_clients_per_station[station_name] = 1

        functions_updates_subject = "$memphis_functions_updates_" + station_name

        if len(first_functions) == 0:
            self.functions_updates_data[station_name] = {}
        else:
            self.functions_updates_data[station_name] = first_functions

        sub = await self.broker_manager.subscribe(functions_updates_subject)
        self.functions_updates_subs[station_name] = sub

        loop = asyncio.get_event_loop()
        task = loop.create_task(
            self.get_msg_functions_updates(
                station_name, self.functions_updates_subs[station_name].messages
            )
        )
        self.functions_tasks[station_name] = task

    async def get_msg_functions_updates(self, station_name, iterable):
        async for msg in iterable:
            message = msg.data.decode("utf-8")
            message = json.loads(message)
            self.functions_updates_data[station_name] = message["functions"]

    async def start_listen_for_schema_updates(self, station_name, schema_update_data):
        schema_updates_subject = "$memphis_schema_updates_" + station_name

        empty = schema_update_data["schema_name"] == ""
        if empty:
            self.schema_updates_data[station_name] = {}
        else:
            self.schema_updates_data[station_name] = schema_update_data

        schema_exists = self.schema_updates_subs.get(station_name)
        if schema_exists:
            self.clients_per_station[station_name] += 1
        else:
            sub = await self.broker_manager.subscribe(schema_updates_subject)
            self.clients_per_station[station_name] = 1
            self.schema_updates_subs[station_name] = sub
        task_exists = self.schema_tasks.get(station_name)
        if not task_exists:
            loop = asyncio.get_event_loop()
            task = loop.create_task(
                self.get_msg_schema_updates(
                    station_name, self.schema_updates_subs[station_name].messages
                )
            )
            self.schema_tasks[station_name] = task

    async def consumer(
        self,
        station_name: str,
        consumer_name: str,
        consumer_group: str = "",
        pull_interval_ms: int = 1000,
        batch_size: int = 10,
        batch_max_time_to_wait_ms: int = 5000,
        max_ack_time_ms: int = 30000,
        max_msg_deliveries: int = 2,
        generate_random_suffix: bool = False,
        start_consume_from_sequence: int = 1,
        last_messages: int = -1,
        timeout_retries=5,
    ):
        """Creates a consumer.
        Args:.
            station_name (str): station name to consume messages from.
            consumer_name (str): name for the consumer.
            consumer_group (str, optional): consumer group name. Defaults to the consumer name.
            pull_interval_ms (int, optional): interval in milliseconds between pulls. Defaults to 1000.
            batch_size (int, optional): pull batch size. Defaults to 10.
            batch_max_time_to_wait_ms (int, optional): max time in milliseconds to wait between pulls. Defaults to 5000.
            max_ack_time_ms (int, optional): max time for ack a message in milliseconds, in case a message not acked in this time period the Memphis broker will resend it. Defaults to 30000.
            max_msg_deliveries (int, optional): max number of message deliveries, by default is 2.
            generate_random_suffix (bool): Deprecated: will be stopped to be supported after November 1'st, 2023. false by default, if true concatenate a random suffix to consumer's name
            start_consume_from_sequence(int, optional): start consuming from a specific sequence. defaults to 1.
            last_messages: consume the last N messages, defaults to -1 (all messages in the station).
        Returns:
            object: consumer
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")
            if batch_size > self.MAX_BATCH_SIZE:
                raise MemphisError(
                    f"Batch size can not be greater than {self.MAX_BATCH_SIZE}")
            real_name = consumer_name.lower()
            if generate_random_suffix:
                warnings.warn("Deprecation warning: generate_random_suffix will be stopped to be supported after November 1'st, 2023.")
                consumer_name = self.__generate_random_suffix(consumer_name)
            cg = consumer_name if not consumer_group else consumer_group

            if start_consume_from_sequence <= 0:
                raise MemphisError(
                    "start_consume_from_sequence has to be a positive number"
                )

            if last_messages < -1:
                raise MemphisError("min value for last_messages is -1")

            if start_consume_from_sequence > 1 and last_messages > -1:
                raise MemphisError(
                    "Consumer creation options can't contain both start_consume_from_sequence and last_messages"
                )
            create_consumer_req = {
                "name": consumer_name,
                "station_name": station_name,
                "connection_id": self.connection_id,
                "consumer_type": "application",
                "consumers_group": consumer_group,
                "max_ack_time_ms": max_ack_time_ms,
                "max_msg_deliveries": max_msg_deliveries,
                "start_consume_from_sequence": start_consume_from_sequence,
                "last_messages": last_messages,
                "req_version": 3,
                "username": self.username,
                "app_id": app_id,
            }

            create_consumer_req_bytes = json.dumps(create_consumer_req, indent=2).encode(
                "utf-8"
            )
            creation_res = await self._request(
                "$memphis_consumer_creations", create_consumer_req_bytes, 20, timeout_retries
            )
            creation_res = creation_res.data.decode("utf-8")
            if creation_res != "":
                try:
                    creation_res = json.loads(creation_res)

                    if creation_res["error"] != "":
                        raise MemphisError(creation_res["error"])
                    internal_station_name = get_internal_name(station_name)

                    if creation_res["partitions_update"]["partitions_list"] is not None:
                        self.partition_consumers_updates_data[internal_station_name] = creation_res["partitions_update"]
                except:
                    raise MemphisError(creation_res)

            inner_station_name = get_internal_name(station_name.lower())

            partition_generator = None

            if inner_station_name in self.partition_consumers_updates_data:
                partition_generator = PartitionGenerator(self.partition_consumers_updates_data[inner_station_name]["partitions_list"])

            consumer_group = get_internal_name(cg.lower())
            subscriptions = {}

            if inner_station_name not in self.partition_consumers_updates_data:
                subject = inner_station_name + ".final"
                psub = await self.broker_connection.pull_subscribe(subject, durable=consumer_group)
                subscriptions[1] = psub
            else:
                for p in self.partition_consumers_updates_data[inner_station_name]["partitions_list"]:
                    subject = f"{inner_station_name}${str(p)}.final"
                    psub = await self.broker_connection.pull_subscribe(subject, durable=consumer_group)
                    subscriptions[p] = psub


            internal_station_name = get_internal_name(station_name)
            map_key = internal_station_name + "_" + real_name
            if "schema_update" in creation_res:
                await self.start_listen_for_schema_updates(
                    internal_station_name, creation_res["schema_update"]
                )
                self.update_schema_data(station_name)
            consumer = Consumer(
                self,
                station_name,
                consumer_name,
                cg,
                pull_interval_ms,
                batch_size,
                batch_max_time_to_wait_ms,
                max_ack_time_ms,
                max_msg_deliveries,
                start_consume_from_sequence=start_consume_from_sequence,
                last_messages=last_messages,
                partition_generator=partition_generator,
                subscriptions=subscriptions,
            )
            self.consumers_map[map_key] = consumer
            return consumer
        except Exception as e:
            raise MemphisError(str(e)) from e

    async def produce(
        self,
        station_name: Union[str, List[str]],
        producer_name: str,
        message,
        generate_random_suffix: bool = False,
        ack_wait_sec: int = 15,
        headers: Union[Headers, None] = None,
        async_produce: bool = False,
        msg_id: Union[str, None] = None,
        producer_partition_key: Union[str, None] = None,
        producer_partition_number: Union[int, -1] = -1
    ):
        """Produces a message into a station without the need to create a producer.
        Args:
            station_name (str): station name to produce messages into.
            producer_name (str): name for the producer.
            message (bytearray/dict): message to send into the station - bytearray/protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema or  bytearray/dict (schema validated station - avro schema))
            generate_random_suffix (bool): Deprecated: will be stopped to be supported after November 1'st, 2023. false by default, if true concatenate a random suffix to producer's name
            ack_wait_sec (int, optional): max time in seconds to wait for an ack from memphis. Defaults to 15.
            headers (dict, optional): Message headers, defaults to {}.
            async_produce (boolean, optional): produce operation won't wait for broker acknowledgement
            msg_id (string, optional): Attach msg-id header to the message in order to achieve idempotence
            producer_partition_key (string, optional): produce to a specific partition using the partition key
            producer_partition_number (int, optional): produce to a specific partition using the partition number
        Raises:
            Exception: _description_
        """
        try:
            if not isinstance(station_name, str) and not isinstance(station_name, list):
                raise MemphisError("station_name should be either string or list of strings")
            if isinstance(station_name, str):
                await self._single_station_produce(station_name, producer_name, message, generate_random_suffix, ack_wait_sec, headers, async_produce, msg_id, producer_partition_key, producer_partition_number)
            else:
                await self._multi_station_produce(station_name, producer_name, message, generate_random_suffix, ack_wait_sec, headers, async_produce, msg_id, producer_partition_key, producer_partition_number)
        except Exception as e:
            raise MemphisError(str(e)) from e


    async def _single_station_produce(
        self,
        station_name: str,
        producer_name: str,
        message,
        generate_random_suffix: bool = False,
        ack_wait_sec: int = 15,
        headers: Union[Headers, None] = None,
        async_produce: bool = False,
        msg_id: Union[str, None] = None,
        producer_partition_key: Union[str, None] = None,
        producer_partition_number: Union[int, -1] = -1
    ):
        try:
            internal_station_name = get_internal_name(station_name)
            map_key = internal_station_name + "_" + producer_name.lower()
            producer = None
            if map_key in self.producers_map:
                producer = self.producers_map[map_key]
            else:
                producer = await self.producer(
                    station_name=station_name,
                    producer_name=producer_name,
                    generate_random_suffix=generate_random_suffix,
                )
            await producer.produce(
                message=message,
                ack_wait_sec=ack_wait_sec,
                headers=headers,
                async_produce=async_produce,
                msg_id=msg_id,
                producer_partition_key=producer_partition_key,
                producer_partition_number=producer_partition_number
            )
        except Exception as e:
            raise MemphisError(str(e)) from e

    async def _multi_station_produce(
        self,
        station_names: List[str],
        producer_name: str,
        message,
        generate_random_suffix: bool = False,
        ack_wait_sec: int = 15,
        headers: Union[Headers, None] = None,
        async_produce: bool = False,
        msg_id: Union[str, None] = None,
        producer_partition_key: Union[str, None] = None,
        producer_partition_number: Union[int, -1] = -1
    ):
        try:
            producer = await self.producer(
                station_name=station_names,
                producer_name=producer_name,
                generate_random_suffix=generate_random_suffix,
            )
            await producer.produce(
                message=message,
                ack_wait_sec=ack_wait_sec,
                headers=headers,
                async_produce=async_produce,
                msg_id=msg_id,
                producer_partition_key=producer_partition_key,
                producer_partition_number=producer_partition_number
            )
        except Exception as e:
            raise MemphisError(str(e)) from e


    async def fetch_messages(
        self,
        station_name: str,
        consumer_name: str,
        consumer_group: str = "",
        batch_size: int = 10,
        batch_max_time_to_wait_ms: int = 5000,
        max_ack_time_ms: int = 30000,
        max_msg_deliveries: int = 2,
        generate_random_suffix: bool = False,
        start_consume_from_sequence: int = 1,
        last_messages: int = -1,
        consumer_partition_key: str = None,
        consumer_partition_number: int = -1,
        prefetch: bool = False,
    ):
        """Consume a batch of messages.
        Args:.
            station_name (str): station name to consume messages from.
            consumer_name (str): name for the consumer.
            consumer_group (str, optional): consumer group name. Defaults to the consumer name.
            batch_size (int, optional): pull batch size. Defaults to 10.
            batch_max_time_to_wait_ms (int, optional): max time in miliseconds to wait between pulls. Defaults to 5000.
            max_ack_time_ms (int, optional): max time for ack a message in miliseconds, in case a message not acked in this time period the Memphis broker will resend it. Defaults to 30000.
            max_msg_deliveries (int, optional): max number of message deliveries, by default is 2.
            generate_random_suffix (bool): Deprecated: will be stopped to be supported after November 1'st, 2023. false by default, if true concatenate a random suffix to consumer's name
            start_consume_from_sequence(int, optional): start consuming from a specific sequence. defaults to 1.
            last_messages: consume the last N messages, defaults to -1 (all messages in the station).
            consumer_partition_key (str): consume from a specific partition using the partition key.
            consumer_partition_number (int): consume from a specific partition using the partition number.
            prefetch: false by default, if true then fetch messages from local cache (if exists) and load more messages into the cache.
        Returns:
            list: Message
        """
        try:
            consumer = None
            if not self.is_connection_active:
                raise MemphisError(
                    "Cant fetch messages without being connected!")
            if batch_size > self.MAX_BATCH_SIZE:
                raise MemphisError(
                    f"Batch size can not be greater than {self.MAX_BATCH_SIZE}")
            internal_station_name = get_internal_name(station_name)
            consumer_map_key = internal_station_name + "_" + consumer_name.lower()
            if consumer_map_key in self.consumers_map:
                consumer = self.consumers_map[consumer_map_key]
            else:
                consumer = await self.consumer(
                    station_name=station_name,
                    consumer_name=consumer_name,
                    consumer_group=consumer_group,
                    batch_size=batch_size,
                    batch_max_time_to_wait_ms=batch_max_time_to_wait_ms,
                    max_ack_time_ms=max_ack_time_ms,
                    max_msg_deliveries=max_msg_deliveries,
                    generate_random_suffix=generate_random_suffix,
                    start_consume_from_sequence=start_consume_from_sequence,
                    last_messages=last_messages,
                )
            messages = await consumer.fetch(batch_size, consumer_partition_key=consumer_partition_key, consumer_partition_number=consumer_partition_number, prefetch=prefetch)
            if messages == None:
                messages = []
            return messages
        except Exception as e:
            raise MemphisError(str(e)) from e

    async def create_schema(self, schema_name, schema_type, schema_path, timeout_retries=5):

        """Creates a new schema. In case schema is already exist a new version will be created
        Args:.
            schema_name (str): the name of the schema.
            schema_type (str): the type of the schema json / graphql / protobuf / avro.
            schema_path (str): the path for the schema file
        """

        if schema_type not in {'json', 'graphql', 'protobuf', 'avro'}:
            raise MemphisError("schema type not supported" + type)

        try:
            await self.schema_name_validation(schema_name)
        except Exception as e:
            raise e


        schema_content = ""
        with open(schema_path, "rt", encoding="utf-8") as f:
            schema_content = f.read()

        create_schema_req = {
            "name": schema_name,
            "type": schema_type,
            "created_by_username": self.username,
            "schema_content": schema_content,
            "message_struct_name": ""
        }

        create_schema_req_bytes = json.dumps(create_schema_req, indent=2).encode("utf-8")

        create_res = await self._request(
            "$memphis_schema_creations", create_schema_req_bytes, 20, timeout_retries)

        create_res = create_res.data.decode("utf-8")
        create_res = json.loads(create_res)
        if create_res["error"] != "" and not "already exists" in create_res["error"] :
            raise MemphisError(create_res["error"])


    async def schema_name_validation(self, schema_name):
        if len(schema_name) == 0:
            raise MemphisError("Schema name cannot be empty")

        if len(schema_name) > 128:
            raise MemphisError("Schema name should be under 128 characters")

        if re.fullmatch(r'^[a-z0-9_.-]*$', schema_name) is None:
            raise MemphisError("Only alphanumeric and the '_', '-', '.' characters are allowed in the schema name")

        if not schema_name[0].isalnum() or not schema_name[-1].isalnum():
            raise MemphisError("Schema name cannot start or end with a non-alphanumeric character")

    def is_connected(self):
        return self.broker_manager.is_connected

    def unset_cached_producer_station(self, station_name):
        try:
            internal_station_name = get_internal_name(station_name)
            for key in list(self.producers_map):
                producer = self.producers_map[key]
                if producer.internal_station_name == internal_station_name:
                    del self.producers_map[key]
        except Exception as e:
            raise e

    def unset_cached_consumer_station(self, station_name):
        try:
            internal_station_name = get_internal_name(station_name)
            for key in list(self.consumers_map):
                consumer = self.consumers_map[key]
                consumer_station_name_internal = get_internal_name(
                    consumer.station_name)
                if consumer_station_name_internal == internal_station_name:
                    del self.consumers_map[key]
        except Exception as e:
            raise e
