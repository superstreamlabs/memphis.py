from memphis import Memphis
from memphis.message import Message
import asyncio
import os

async def main():
    try:
        # Connecting to the broker
        memphis = Memphis()
    
        await memphis.connect(
          host = "aws-us-east-1.cloud.memphis.dev",
          username = "test_user",
          password = os.environ.get("memphis_pass"),
          account_id = os.environ.get("memphis_account_id") # For cloud users on, at the top of the overview page
        )  

        # Creating a producer and producing a message. You can also use the memphis.producer function
        producer = await memphis.producer(
            station_name = "test_station", # Matches the station name in memphis cloud
            producer_name = "producer"
        )

        for i in range(10):
            await producer.produce(
                message={
                    "id": i,
                    "chocolates_to_eat": 3
                }
            )

    except Exception as e:
        print(e)
    finally:
        await memphis.close()

if __name__ == '__main__':
  asyncio.run(main()) 