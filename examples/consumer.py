import asyncio
from memphis import Memphis


async def main():
    def msg_handler(msg):
        print("message: ", msg.get_data())
        msg.ack()

    def error_handler(error):
        print("error: ", error)
    try:
        memphis = Memphis()
        await memphis.connect(host="<memphis-host>", username="<application type username>", connection_token="<broker-token>")

        consumer = await memphis.consumer(station_name="<station-name>", consumer_name="<consumer-name>", consumer_group="")
        consumer.event.on("message", msg_handler)
        consumer.event.on("error", error_handler)
        await consumer.consume()

        await asyncio.sleep(10)
        await memphis.close()
    except Exception as e:
        print(e)
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())
