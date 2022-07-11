import asyncio
from time import sleep
from memphis import Memphis
from memphis import retention_types, storage_types
from threading import Thread


async def main():
    storage_types.FILE
    # global counter
    # # counter = 0

    def msg_handler_1(msg):
        print("message_1: ", msg.get_data())
        msg.ack()

    def msg_handler_2(msg):
        print("message_2: ", msg.get_data())
        msg.ack()
        # global counter
        # counter += 1
        # if counter >= 60:
        #     raise Exception("done")

    def error_handler(error):
        print("error: ", error)

    try:
        memphis = Memphis()
        await memphis.connect(host="http://localhost", username="sveta", connection_token="memphis")

        factory = await memphis.factory(name="faccc")
        station = await memphis.station(name="stationnnnnn", factory_name=factory.name)
        prod = await memphis.producer(station_name=station.name, producer_name="sveta")
        for i in range(40):
            # await prod.produce(bytearray(i))
            await prod.produce(bytearray("This is a msg num "+str(i), 'utf-8'))
        cons22 = await memphis.consumer(
            station_name=station.name,
            consumer_name="osr",
            consumer_group="ss",  # defaults to ""
            pull_interval_ms=500,  # defaults to 1000
            batch_size=10,  # defaults to 10
        )
        cons22.event.on("message", msg_handler_2)
        cons22.event.on("error", error_handler)

        cons11 = await memphis.consumer(
            station_name=station.name,
            consumer_name="sve",
            consumer_group="cg",  # defaults to ""
            pull_interval_ms=500,  # defaults to 1000
            batch_size=10,  # defaults to 10
        )
        cons11.event.on("message", msg_handler_1)
        cons11.event.on("error", error_handler)

        # cons22.consume()
        # cons11.consume()

        # # task1.cancel()
        await asyncio.sleep(15)
        await memphis.close()

    except Exception as e:
        print(e)
        await memphis.close()


async def print_something():
    await asyncio.sleep(1)
    print("Working")

if __name__ == '__main__':
    asyncio.run(main())
