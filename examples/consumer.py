import asyncio
from memphis import Memphis


async def main():
    async def msg_handler(msg):
        print("message: ", msg.get_data())
        await msg.ack()

    try:
        memphis = Memphis()
        await memphis.connect(host="<memphis-host>", username="<application type username>", connection_token="<broker-token>")

        consumer = await memphis.consumer(station_name="<station-name>", consumer_name="<consumer-name>", consumer_group="")
        consumer.consume(msg_handler)
        await asyncio.sleep(5)

    except Exception as e:
        print(e)

    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())
