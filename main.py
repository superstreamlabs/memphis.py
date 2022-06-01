from multiprocessing import connection
from os import execv
import socket
import json
import asyncio
from sqlite3 import connect
import sys
import nats as broker
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
import http_request
from time import time, sleep
import uuid
from pymitter import EventEmitter


async def main():
    ee = EventEmitter()

    class Memphis:
        def __init__(self):
            self.is_connection_active = False
            self.connection_id = None
            self.access_token = None
            self.host = None
            self.management_port = 5555
            self.tcp_port = 6666
            self.data_port = 7766
            self.username = None
            self.connection_token = None
            self.access_token_timeout = None
            self.ping_timeout = None
            self.client = socket.socket()
            self.reconnect_attempts = 0
            self.reconnect = True
            self.max_reconnect = 3
            self.reconnect_interval_ms = 200
            self.timeout_ms = 15000
            self.broker_connection = None
            self.broker_manager = None
            self.connected = False

        async def connect(self, host, username, connection_token, management_port=5555, tcp_port=6666, data_port=7766, reconnect=True, max_reconnect=3, reconnect_interval_ms=200, timeout_ms=15000):
            self.host = self.__normalize_host(host)
            self.management_port = management_port
            self.tcp_port = tcp_port
            self.data_port = data_port
            self.username = username
            self.connection_token = connection_token
            self.reconnect = reconnect
            self.max_reconnect = 9 if max_reconnect > 9 else max_reconnect
            self.reconnect_interval_ms = reconnect_interval_ms
            self.timeout_ms = timeout_ms  # check interval
            self.client.connect((self.host, self.tcp_port))
            connection_details = {"username": self.username,
                                  "broker_creds": self.connection_token, "connection_id": None}
            self.client.send(json.dumps(connection_details).encode())
            data = self.client.recv(1024)
            try:
                data = json.loads(data)
            except Exception as e:
                raise Exception(data)
            self.connection_id = data['connection_id']
            self.is_connection_active = True
            self.reconnect_attempts = 0

            if data['access_token']:
                self.access_token = data['access_token']
                # self.__keep_acess_token_fresh(data['access_token_exp'])

            # if data['ping_interval_ms']:
                # if (data.ping_interval_ms) this._pingServer(data.ping_interval_ms); data['ping_interval_ms']

            if not self.connected:
                try:
                    self.broker_manager = await broker.connect(servers=self.host+":"+str(self.data_port), allow_reconnect=True, reconnect_time_wait=2, connect_timeout=2, max_reconnect_attempts=60, token="memphis")
                    self.broker_connection = self.broker_manager.jetstream()
                    self.connected = True
                except Exception as e:
                    raise Exception(e)

        async def factory(self, name, description=""):
            try:
                if not self.connected:
                    raise Exception("Connection is dead")

                response = http_request("POST", 'http://'+self.host+':' + str(self.management_port)+'/api/factories/createFactory',
                                        headers={"Authorization": "Bearer " + self.access_token}, body_params={"name": name, "description": description})
                return Factory(self, json.loads(response)['name'])
            except Exception as e:
                print(e)  # ??
                raise Exception(e)

        async def station(self, name, factory_name, retention_type="message_age_sec", retention_value=604800, storage_type="file", replicas=1, dedup_enabled=False, dedup_window_ms=0):
            try:
                if not self.is_connection_active:
                    raise Exception("Connection is dead")

                response = http_request("POST", 'http://'+self.host+':' + str(self.management_port)+'/api/stations/createStation', headers={"Authorization": "Bearer " + self.access_token}, body_params={
                    "name": name, "factory_name": factory_name, "retention_type": retention_type, "retention_value": retention_value, "storage_type": storage_type, "replicas": replicas, "dedup_enabled": dedup_enabled, "dedup_window_in_ms": dedup_window_ms})
                return Station(self, json.loads(response)['name'])
            except Exception as e:
                print(e)  # ??
                raise Exception(e)

        async def producer(self, station_name, producer_name):
            try:
                if not self.is_connection_active:
                    raise Exception("Connection is dead")

                http_request("POST", 'http://'+self.host+':' + str(self.management_port)+'/api/producers/createProducer', headers={"Authorization": "Bearer " + self.access_token}, body_params={
                    "name": producer_name, "station_name": station_name, "connection_id": self.connection_id, "producer_type": "application"})
                return Producer(self, producer_name, station_name)
            except Exception as e:
                raise Exception(e)

        async def consumer(self, station_name, consumer_name, consumer_group="", pull_interval_ms=1000, batch_size=10, batch_max_time_to_wait_ms=5000, max_ack_time_ms=30000):
            try:
                if not self.connected:
                    raise Exception("Connection is dead")

                http_request("POST", 'http://'+self.host+':' + str(self.management_port)+'/api/consumers/createConsumer', headers={"Authorization": "Bearer " + self.access_token}, body_params={
                    "name": consumer_name, "station_name": station_name, "connection_id": self.connection_id, "consumer_type": "application", "consumers_group": consumer_group, "max_ack_time_ms": max_ack_time_ms})
                return Consumer(self, station_name, consumer_name, consumer_group, pull_interval_ms, batch_size, batch_max_time_to_wait_ms, max_ack_time_ms)
            except Exception as e:
                raise Exception(e)

        def __normalize_host(self, host):
            if (host.startswith("http://")):
                return host.split("http://")[1]
            elif (host.startswith("https://")):
                return host.split("https://")[1]
            else:
                return host

        def __keep_acess_token_fresh(self, expires_in):
            print(expires_in)
            # TODO

    class Factory:
        def __init__(self, connection, name):
            self.connection = connection
            self.name = name.lower()

        def destroy(self):
            try:
                http_request("DELETE", 'http://'+self.connection.host+':'+str(self.connection.management_port)+'/api/factories/removeFactory', headers={
                    "Authorization": "Bearer " + self.connection.access_token}, body_params={"factory_name": self.name})
            except Exception as e:
                raise Exception(e)

    class Station:
        def __init__(self, connection, name):
            self.connection = connection
            self.name = name.lower()

        def destroy(self):
            try:
                http_request("DELETE", 'http://'+self.connection.host+':'+str(self.connection.management_port)+'/api/stations/removeStation', headers={
                    "Authorization": "Bearer " + self.connection.access_token}, body_params={"station_name": self.name})
            except Exception as e:
                raise Exception(e)

    class Producer:
        def __init__(self, connection, producer_name, station_name):
            self.connection = connection
            self.producer_name = producer_name
            self.station_name = station_name

        async def produce(self, message, ack_wait_sec=15):
            try:
                await self.connection.broker_connection.publish(self.station_name + ".final", message.encode('ascii'), headers={
                    "Nats-Msg-Id": str(uuid.uuid4())
                })
            except Exception as e:
                if e.status_code == '503':
                    raise Exception(
                        "Produce operation has failed, please check wheether Station/Producer are still exist")
                else:
                    raise Exception(e)

        async def destroy(self):
            try:
                http_request("DELETE", 'http://'+self.connection.host+':'+str(self.connection.management_port)+'/api/consumers/destroyProducer', headers={
                             "Authorization": "Bearer " + self.connection.access_token}, body_params={"name": self.producer_name, "station_name": self.station_name})
            except:
                return

    class Consumer:
        def __init__(self, connection,
                     station_name,
                     consumer_name,
                     consumer_group,
                     pull_interval_ms,
                     batch_size,
                     batch_max_time_to_wait_ms,
                     max_ack_time_ms):
            self.connection = connection
            self.station_name = station_name.lower()
            self.consumer_name = consumer_name.lower()
            self.consumer_group = consumer_group.lower()
            self.pull_interval_ms = pull_interval_ms
            self.batch_size = batch_size
            self.batch_max_time_to_wait_ms = batch_max_time_to_wait_ms
            self.max_ack_time_ms = max_ack_time_ms

        # async def subscribe(self):
            # self.psub = self.connection.broker_connection.pull_subscribe(
            #     self.station_name + ".final", "psub")
            # while True:
            #     # run every 1 second... you can change that
            #     sleep((self.pull_interval_ms/1000) - time() %
            #           (self.pull_interval_ms/1000))
            #     msgs = self.psub.fetch(self.batch_size)
            #     for msg in msgs:
            #         # self.emitter.emit('data', msg.data.decode())
            #         ee.emit("message", msg.data.decode())
            #         print(msg.data.decode())

    memphis = Memphis()
    connected = False
    await memphis.connect(
        "http://localhost", "sveta", "memphis", max_reconnect=12)
    # fac = await memphis.factory("fact", "dddd")
    # fac.destroy()

    # sta = await memphis.station("myStation1", "fact")
    # sta.destroy()

    # prod = await memphis.producer("new_station", "myProd")
    # for i in range(3000, 4000):
    #     await prod.produce("this is a msg num "+str(i))
    # await prod.destroy()

    # def msg_handler(arg):
    #     print("message", arg)
    # ee.on("message", msg_handler)

    # await memphis.factory("fact", "description")
    # await memphis.station("new_station", "fact")
    await memphis.consumer("new_station", "check_cons")

    # cons = await memphis.consumer("new_station", "check_cons")
    # await cons.subscribe()

    # ee.emit("message", "bar")

if __name__ == '__main__':
    asyncio.run(main())
