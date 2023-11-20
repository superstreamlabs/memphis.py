from __future__ import annotations

import asyncio
import json
import mmh3

from memphis.exceptions import MemphisError
from memphis.utils import default_error_handler, get_internal_name
from memphis.message import Message
from memphis.partition_generator import PartitionGenerator


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
        max_msg_deliveries: int = 2,
        error_callback=None,
        start_consume_from_sequence: int = 1,
        last_messages: int = -1,
        partition_generator: PartitionGenerator = None,
        subscriptions: dict = None,
    ):
        self.connection = connection
        self.station_name = station_name.lower()
        self.internal_station_name = get_internal_name(self.station_name)
        self.consumer_name = consumer_name.lower()
        self.consumer_group = consumer_group.lower()
        self.pull_interval_ms = pull_interval_ms
        self.batch_size = batch_size
        self.batch_max_time_to_wait_ms = batch_max_time_to_wait_ms
        self.max_ack_time_ms = max_ack_time_ms
        self.max_msg_deliveries = max_msg_deliveries
        self.ping_consumer_interval_ms = 30000
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
        self.inner_station_name = get_internal_name(self.station_name)
        self.subscriptions = subscriptions
        self.partition_generator = partition_generator
        self.cached_messages = []
        self.loading_thread = None


    def set_context(self, context):
        """Set a context (dict) that will be passed to each message handler call."""
        self.context = context

    def consume(self, callback, consumer_partition_key: str = None, consumer_partition_number: int = -1):
        """
        This method starts consuming events from the specified station and invokes the provided callback function for each batch of messages received.

        Parameters:
            callback (function): A function that will be called with each batch of messages received. The function should have the following signature:
                - `callback(messages: List[Message], error: Optional[MemphisError], context: Dict) -> Awaitable[None]`
                - `messages`: A list of `Message` objects representing the batch of messages received.
                - `error`: An optional `MemphisError` object if there was an error while consuming the messages.
                - `context`: A dictionary representing the context that was set using the `set_context()` method.
            consumer_partition_key (str): A string that will be used to determine the partition to consume from. If not provided, the consume will work in a Round Robin fashion.
            consumer_partition_number (int): An integer that will be used to determine the partition to consume from. If not provided, the consume will work in a Round Robin fashion.

        Example:
            import asyncio
            from memphis import Memphis

            async def message_handler(messages, error, context):
                if error:
                    print(f"Error occurred: {error}")
                    return

                for message in messages:
                    print(f"Received message: {message}")

            async def main():
                memphis = Memphis()
                await memphis.connect(host='localhost', username='user', password='pass')
                consumer = await memphis.consumer(station_name='my_station', consumer_name='my_consumer', consumer_group='my_group')
                consumer.set_context({'key': 'value'})
                consumer.consume(message_handler)

                # Keep the event loop running
                while True:
                    await asyncio.sleep(1)
            asyncio.run(main())
        """
        self.dls_callback_func = callback
        self.t_consume = asyncio.create_task(self.__consume(callback, partition_key=consumer_partition_key, consumer_partition_number=consumer_partition_number))

    async def __consume(self, callback, partition_key: str = None, consumer_partition_number: int = -1):
        partition_number = 1
        if consumer_partition_number > 0 and partition_key is not None:
            raise MemphisError('Can not use both partition number and partition key')
        elif partition_key is not None:
            partition_number = self.get_partition_from_key(partition_key)
        elif consumer_partition_number > 0:
            self.validate_partition_number(consumer_partition_number, self.inner_station_name)
            partition_number = consumer_partition_number

        while True:
            if self.connection.is_connection_active and self.pull_interval_ms:
                try:
                    if len(self.subscriptions) > 1:
                        if partition_key is None and consumer_partition_number < 1:
                            partition_number = next(self.partition_generator)

                    memphis_messages = []
                    msgs = await self.subscriptions[partition_number].fetch(self.batch_size)

                    for msg in msgs:
                        memphis_messages.append(
                            Message(msg, self.connection, self.consumer_group, self.internal_station_name)
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
                        msg, self.connection, self.consumer_group, self.internal_station_name)
                )
                self.dls_current_index += 1
                if self.dls_callback_func != None:
                    await self.dls_callback_func(
                        [Message(msg, self.connection, self.consumer_group, self.internal_station_name)],
                        None,
                        self.context,
                    )
        except Exception as e:
            if self.dls_callback_func != None:
                await self.dls_callback_func([], MemphisError(str(e)), self.context)
                return

    async def fetch(self, batch_size: int = 10, consumer_partition_key: str = None, consumer_partition_number: int = -1, prefetch: bool = False):
        """
        Fetch a batch of messages.

        Returns a list of Message objects. If the connection is
        not active or no messages are recieved before timing out,
        an empty list is returned.

        Example:

            import asyncio
            
            from memphis import Memphis

            async def main(host, username, password, station):
                memphis = Memphis()
                await memphis.connect(host=host,
                                      username=username,
                                      password=password)
            
                consumer = await memphis.consumer(station_name=station,
                                                  consumer_name="test-consumer",
                                                  consumer_group="test-consumer-group")
            
                while True:
                    batch = await consumer.fetch()
                    print("Recieved {} messages".format(len(batch)))
                    for msg in batch:
                        serialized_record = msg.get_data()
                        print("Message:", serialized_record)
            
                await memphis.close()

            if __name__ == '__main__':
                asyncio.run(main(host,
                                 username,
                                 password,
                                 station))
        
        """
        messages = []
        partition_number = 1
        if len(self.subscriptions) > 1:
            if consumer_partition_number > 0 and consumer_partition_key is not None:
                raise MemphisError('Can not use both partition number and partition key')
            elif consumer_partition_key is not None:
                partition_number = self.get_partition_from_key(consumer_partition_key)
            elif consumer_partition_number > 0:
                self.validate_partition_number(consumer_partition_number, self.inner_station_name)
                partition_number = consumer_partition_number
            else:
                partition_number = next(self.partition_generator)


        if prefetch and len(self.cached_messages) > 0:
            if len(self.cached_messages) >= batch_size:
                messages = self.cached_messages[:batch_size]
                self.cached_messages = self.cached_messages[batch_size:]
                number_of_messages_to_prefetch = batch_size * 2 - batch_size  # calculated for clarity
                self.load_messages_to_cache(number_of_messages_to_prefetch, partition_number)
                return messages
            else:
                messages = self.cached_messages
                batch_size -= len(self.cached_messages)
                self.cached_messages = []

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
                msgs = await self.subscriptions[partition_number].fetch(batch_size)
                for msg in msgs:
                    messages.append(
                        Message(msg, self.connection, self.consumer_group, self.internal_station_name))
                if prefetch:
                    number_of_messages_to_prefetch = batch_size * 2
                    self.load_messages_to_cache(number_of_messages_to_prefetch, partition_number)
                return messages
            except Exception as e:
                if "timeout" not in str(e).lower():
                    raise MemphisError(str(e)) from e

        return messages


    async def __ping_consumer(self, callback):
        while True:
            try:
                await asyncio.sleep(self.ping_consumer_interval_ms / 1000)
                station_inner = get_internal_name(self.station_name)
                consumer_group = get_internal_name(self.consumer_group)
                if self.inner_station_name in self.connection.partition_consumers_updates_data:
                    for p in self.connection.partition_consumers_updates_data[station_inner]["partitions_list"]:
                        stream_name = f"{station_inner}${str(p)}"
                        await self.connection.broker_connection.consumer_info(
                            stream_name, consumer_group, timeout=30
                        )
                else:
                    stream_name = station_inner
                    await self.connection.broker_connection.consumer_info(
                        stream_name, consumer_group, timeout=30
                    )

            except Exception as e:
                if "consumer not found" in str(e) or "stream not found" in str(e):
                    callback(MemphisError(str(e)))

    async def destroy(self, timeout_retries=5):
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
                "username": self.connection.username,
                "connection_id": self.connection.connection_id,
                "req_version": 1,
            }
            consumer_name = json.dumps(
                destroy_consumer_req, indent=2).encode("utf-8")
            # pylint: disable=protected-access
            res = await self.connection._request(
                "$memphis_consumer_destructions", consumer_name, 5, timeout_retries
            )
            # pylint: enable=protected-access
            error = res.data.decode("utf-8")
            if error != "" and not "not exist" in error:
                raise MemphisError(error)
            self.dls_messages.clear()
            internal_station_name = get_internal_name(self.station_name)
            if self.connection.schema_updates_data != {}:
                clients_number = (
                    self.connection.clients_per_station.get(
                        internal_station_name) - 1
                )
                self.connection.clients_per_station[
                    internal_station_name
                ] = clients_number

                if clients_number == 0:
                    sub = self.connection.schema_updates_subs.get(
                        internal_station_name)
                    task = self.connection.schema_tasks.get(internal_station_name)
                    if internal_station_name in self.connection.schema_updates_data:
                        del self.connection.schema_updates_data[internal_station_name]
                    if internal_station_name in self.connection.schema_updates_subs:
                        del self.connection.schema_updates_subs[internal_station_name]
                    if internal_station_name in self.connection.schema_tasks:
                        del self.connection.schema_tasks[internal_station_name]
                    if task is not None:
                        task.cancel()
                    if sub is not None:
                        await sub.unsubscribe()

            map_key = internal_station_name + "_" + self.consumer_name.lower()
            del self.connection.consumers_map[map_key]
        except Exception as e:
            raise MemphisError(str(e)) from e

    def get_partition_from_key(self, key):
        try:
            index = mmh3.hash(key, self.connection.SEED, signed=False) % len(self.subscriptions)
            return self.connection.partition_consumers_updates_data[self.inner_station_name]["partitions_list"][index]
        except Exception as e:
            raise e

    def validate_partition_number(self, partition_number, station_name):
        partitions_list = self.connection.partition_consumers_updates_data[station_name]["partitions_list"]
        if partitions_list is not None:
            if partition_number < 1 or partition_number > len(partitions_list):
                raise MemphisError("Partition number is out of range")
            elif partition_number not in partitions_list:
                raise MemphisError(f"Partition {str(partition_number)} does not exist in station {station_name}")
        else:
            raise MemphisError(f"Partition {str(partition_number)} does not exist in station {station_name}")

    def load_messages_to_cache(self, batch_size, partition_number):
        if not self.loading_thread or not self.loading_thread.is_alive():
            asyncio.ensure_future(self.__load_messages(batch_size, partition_number))

    async def __load_messages(self, batch_size, partition_number):
        new_messages = await self.fetch(batch_size, consumer_partition_number=partition_number)
        if new_messages is not None:
            self.cached_messages.extend(new_messages)
