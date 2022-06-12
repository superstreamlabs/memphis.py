from multiprocessing import connection
from os import execv
from re import S
import socket
import json
import asyncio
from sqlite3 import connect
import sys
from tkinter.messagebox import NO
import nats as broker
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
import time
import uuid
from pymitter import EventEmitter
from memphis.http_request import http_request
from threading import Thread
import threading


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
        self.client = socket.socket()
        self.reconnect_attempts = 0
        self.reconnect = True
        self.max_reconnect = 3
        self.reconnect_interval_ms = 200
        self.timeout_ms = 15000
        self.broker_connection = None
        self.broker_manager = None
        self.connected = False
        self.access_token_exp = None
        self.ping_interval_ms = None

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
        try:
            self.client.connect((self.host, self.tcp_port))
            connection_details = {"username": self.username,
                                  "broker_creds": self.connection_token, "connection_id": None}
        except OSError as msg:
            self.client = None
            self.__close()

        if self.client is None:
            raise Exception("could not open socket")
        self.client.send(json.dumps(connection_details).encode())
        data = self.client.recv(1024)
        try:
            data = json.loads(data)
        except Exception as e:
            raise Exception(data)
        self.connection_id = data['connection_id']
        self.access_token_exp = data['access_token_exp']

        self.is_connection_active = True
        self.reconnect_attempts = 0

        if data['access_token']:
            self.access_token = data['access_token']
            t_keep_acess_token_fresh = Thread(
                target=self.__keep_acess_token_fresh)
            t_keep_acess_token_fresh.start()

        if data['ping_interval_ms']:
            self.ping_interval_ms = data['ping_interval_ms']
            t_ping_interval_ms = Thread(
                target=self.__ping_server)
            t_ping_interval_ms.start()

        if not self.connected:
            try:
                self.broker_manager = await broker.connect(servers=self.host+":"+str(self.data_port), allow_reconnect=True, reconnect_time_wait=2, connect_timeout=2, max_reconnect_attempts=60, token="memphis")
                self.broker_connection = self.broker_manager.jetstream()
                self.connected = True
            except Exception as e:
                raise Exception(e)

    async def factory(self, name, description=""):
        print("=======factory=========")
        try:
            if not self.connected:
                raise Exception("Connection is dead")

            response = http_request("POST", 'http://'+self.host+':' + str(self.management_port)+'/api/factories/createFactory',
                                    headers={"Authorization": "Bearer " + self.access_token}, body_params={"name": name, "description": description})
            return Factory(self, json.loads(response)['name'])
        except Exception as e:
            if str(e).find('already exist') != -1:
                return Factory(self, name.lower())
            else:
                raise Exception(e)

    async def station(self, name, factory_name, retention_type="message_age_sec", retention_value=604800, storage_type="file", replicas=1, dedup_enabled=False, dedup_window_ms=0):
        try:
            if not self.is_connection_active:
                raise Exception("Connection is dead")

            response = http_request("POST", 'http://'+self.host+':' + str(self.management_port)+'/api/stations/createStation', headers={"Authorization": "Bearer " + self.access_token}, body_params={
                "name": name, "factory_name": factory_name, "retention_type": retention_type, "retention_value": retention_value, "storage_type": storage_type, "replicas": replicas, "dedup_enabled": dedup_enabled, "dedup_window_in_ms": dedup_window_ms})
            return Station(self, json.loads(response)['name'])
        except Exception as e:
            if str(e).find('already exist') != -1:
                return Station(self, name.lower())
            else:
                raise Exception(e)

    async def close(self):
        if self.is_connection_active:
            self.client.close()
            self.access_token_timeout = None
            self.ping_interval_ms = None
            self.access_token_exp = None
            self.access_token = None
            self.connection_id = None
            self.is_connection_active = False
            self.reconnect_attempts = 0
            await self.broker_manager.close()
            self.broker_manager = None

    def __normalize_host(self, host):
        if (host.startswith("http://")):
            return host.split("http://")[1]
        elif (host.startswith("https://")):
            return host.split("https://")[1]
        else:
            return host

    def __keep_acess_token_fresh(self):
        starttime = time.time()
        while True:
            if not self.access_token_exp:
                break
            time.sleep(self.access_token_exp/1000 -
                       ((time.time() - starttime) % self.access_token_exp/1000))
            if self.is_connection_active:
                self.client.send(json.dumps(
                    {"resend_access_token": True}).encode())

    def __ping_server(self):
        starttime = time.time()
        while True:
            if not self.ping_interval_ms:
                break
            time.sleep(self.ping_interval_ms/1000 -
                       ((time.time() - starttime) % self.ping_interval_ms/1000))
            if self.is_connection_active:
                self.client.send(json.dumps(
                    {"ping": True}).encode())

    async def __close(self):
        if self.reconnect is True and self.reconnect_attempts < self.max_reconnect:
            self.reconnect_attempts += 1
            starttime = time.time()
            while True:
                time.sleep(self.reconnect_interval_ms/1000 -
                           ((time.time() - starttime) % self.reconnect_interval_ms/1000))
                try:
                    await self.connect(host=self.host, management_port=self.management_port, tcp_port=self.tcp_port, data_port=self.data_port, username=self.username, connection_token=self.connection_token, reconnect=self.reconnect, max_reconnect=self.max_reconnect, reconnect_interval_ms=self.reconnect_interval_ms, timeout_ms=self.timeout_ms)
                    print("Reconnect to memphis has been succeeded")
                except Exception as e:
                    print("Failed reconnect to memphis")
                    return
        elif self.is_connection_active is True:
            self.client.destroy()
            self.access_token = None
            self.connection_id = None
            self.is_connection_active = False
            self.access_token_timeout = None
            self.reconnect_attempts = 0
            starttime = time.time()
            while True:
                time.sleep(0.5 - ((time.time() - starttime) % 0.5))
                if self.broker_manager is True:
                    self.broker_manager.close()

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
    def __init__(self, connection, station_name, consumer_name, consumer_group, pull_interval_ms, batch_size, batch_max_time_to_wait_ms, max_ack_time_ms):
        self.connection = connection
        self.station_name = station_name.lower()
        self.consumer_name = consumer_name.lower()
        self.consumer_group = consumer_group.lower()
        self.pull_interval_ms = pull_interval_ms
        self.batch_size = batch_size
        self.batch_max_time_to_wait_ms = batch_max_time_to_wait_ms
        self.max_ack_time_ms = max_ack_time_ms
        self.event = EventEmitter()

    async def consume(self):
        try:
            self.psub = await self.connection.broker_connection.pull_subscribe(
                self.station_name + ".final", "psub")
            starttime = time.time()
            while True:
                time.sleep(self.pull_interval_ms/1000 -
                           ((time.time() - starttime) % self.pull_interval_ms/1000))
                msgs = await self.psub.fetch(self.batch_size)
                for msg in msgs:
                    self.event.emit('message', Message(msg))
        except Exception as e:
            self.event.emit('error', e)


class Message:
    def __init__(self, message):
        self.message = message

    def ack(self):
        self.message.ack()

    def get_data(self):
        return self.message.data.decode()
