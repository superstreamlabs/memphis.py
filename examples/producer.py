import asyncio
from memphis import Memphis


async def main():
    try:
        memphis = Memphis()
        await memphis.connect(host="<memphis-host>", username="<application type username>", connection_token="<broker-token>")

        producer = memphis.producer(
            station_name="<station-name>", producer_name="<producer-name>")
        for i in range(100):
            await producer.produce(bytearray('Message #'+str(i)+': Hello world', 'utf-8'))

    except Exception as e:
        print(e)

    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())