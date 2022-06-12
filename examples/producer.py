
import asyncio
from memphis import Memphis


async def main():
    try:
        memphis = Memphis()
        await memphis.connect(host="<memphis-host>", username="<application type username>", connection_token="<broker-token>")

        prod = await memphis.producer(station_name="<station-name>", producer_name="<producer-name>")
        for i in range(100):
            await prod.produce("Message #"+str(i)+": Hello world")

        await memphis.close()
    except Exception as e:
        print(e)
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())
