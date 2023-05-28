from __future__ import annotations

import asyncio
import json

from memphis.exceptions import MemphisError
from memphis.utils import default_error_handler, get_internal_name
from memphis.message import Message


class Consumer:
    MAX_BATCH_SIZE = 5000

    def __init__(
        self,
        connection,
        station_name: str,
        consumer_name,
        consumer_group,
        pull_interval_ms: int,
        batch_size: int,
        batch_max_time_to_wait_ms: int,
        max_ack_time_ms: int,
        max_msg_deliveries: int = 10,
        error_callback=None,
        start_consume_from_sequence: int = 1,
        last_messages: int = -1,
    ):
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
        self.last_messages = last_messages
        self.context = {}
        self.dls_messages = []
        self.dls_current_index = 0
        self.dls_callback_func = None
        self.t_dls = asyncio.create_task(self.__consume_dls())
        self.t_consume = None

    def set_context(self, context):
        """Set a context (dict) that will be passed to each message handler call."""
        self.context = context

    def consume(self, callback):
        """Consume events."""
        self.dls_callback_func = callback
        self.t_consume = asyncio.create_task(self.__consume(callback))

    async def __consume(self, callback):
        subject = get_internal_name(self.station_name)
        consumer_group = get_internal_name(self.consumer_group)
        self.psub = await self.connection.broker_connection.pull_subscribe(
            subject + ".final", durable=consumer_group
        )
        while True:
            if self.connection.is_connection_active and self.pull_interval_ms:
                try:
                    memphis_messages = []
                    msgs = await self.psub.fetch(self.batch_size)
                    for msg in msgs:
                        memphis_messages.append(
                            Message(msg, self.connection, self.consumer_group)
                        )
                    await callback(memphis_messages, None, self.context)
                    await asyncio.sleep(self.pull_interval_ms / 1000)

                except asyncio.TimeoutError:
                    await callback(
                        [], MemphisError("Memphis: TimeoutError"), self.context
                    )
                    continue
                except Exception as e:
                    if self.connection.is_connection_active:
                        raise MemphisError(str(e)) from e
                    return
            else:
                break

    async def __consume_dls(self):
        subject = get_internal_name(self.station_name)
        consumer_group = get_internal_name(self.consumer_group)
        try:
            subscription_name = "$memphis_dls_" + subject + "_" + consumer_group
            self.consumer_dls = await self.connection.broker_manager.subscribe(
                subscription_name, subscription_name
            )
            async for msg in self.consumer_dls.messages:
                index_to_insert = self.dls_current_index
                if index_to_insert >= 10000:
                    index_to_insert %= 10000
                self.dls_messages.insert(
                    index_to_insert, Message(
                        msg, self.connection, self.consumer_group)
                )
                self.dls_current_index += 1
                if self.dls_callback_func != None:
                    await self.dls_callback_func(
                        [Message(msg, self.connection, self.consumer_group)],
                        None,
                        self.context,
                    )
        except Exception as e:
            if self.dls_callback_func != None:
                await self.dls_callback_func([], MemphisError(str(e)), self.context)
                return

    async def fetch(self, batch_size: int = 10):
        """Fetch a batch of messages."""
        messages = []
        if self.connection.is_connection_active:
            try:
                if batch_size > self.MAX_BATCH_SIZE:
                    raise MemphisError(
                        f"Batch size can not be greater than {self.MAX_BATCH_SIZE}")
                self.batch_size = batch_size
                if len(self.dls_messages) > 0:
                    if len(self.dls_messages) <= batch_size:
                        messages = self.dls_messages
                        self.dls_messages = []
                        self.dls_current_index = 0
                    else:
                        messages = self.dls_messages[0:batch_size]
                        del self.dls_messages[0:batch_size]
                        self.dls_current_index -= len(messages)
                    return messages

                durable_name = ""
                if self.consumer_group != "":
                    durable_name = get_internal_name(self.consumer_group)
                else:
                    durable_name = get_internal_name(self.consumer_name)
                subject = get_internal_name(self.station_name)
                self.psub = await self.connection.broker_connection.pull_subscribe(
                    subject + ".final", durable=durable_name
                )
                msgs = await self.psub.fetch(batch_size)
                for msg in msgs:
                    messages.append(
                        Message(msg, self.connection, self.consumer_group))
                return messages
            except Exception as e:
                if "timeout" not in str(e):
                    raise MemphisError(str(e)) from e
        else:
            return messages

    async def __ping_consumer(self, callback):
        while True:
            try:
                await asyncio.sleep(self.ping_consumer_invterval_ms / 1000)
                consumer_group = get_internal_name(self.consumer_group)
                await self.connection.broker_connection.consumer_info(
                    self.station_name, consumer_group, timeout=30
                )

            except Exception as e:
                callback(MemphisError(str(e)))

    async def destroy(self):
        """Destroy the consumer."""
        if self.t_consume is not None:
            self.t_consume.cancel()
        if self.t_dls is not None:
            self.t_dls.cancel()
        if self.t_ping is not None:
            self.t_ping.cancel()
        self.pull_interval_ms = None
        try:
            destroy_consumer_req = {
                "name": self.consumer_name,
                "station_name": self.station_name,
                "username": self.connection.username
            }
            consumer_name = json.dumps(
                destroy_consumer_req, indent=2).encode("utf-8")
            res = await self.connection.broker_manager.request(
                "$memphis_consumer_destructions", consumer_name, timeout=5
            )
            error = res.data.decode("utf-8")
            if error != "" and not "not exist" in error:
                raise MemphisError(error)
            self.dls_messages.clear()
            internal_station_name = get_internal_name(self.station_name)
            map_key = internal_station_name + "_" + self.consumer_name.lower()
            del self.connection.consumers_map[map_key]
        except Exception as e:
            raise MemphisError(str(e)) from e
