from memphis import Memphis
from memphis.message import Message
import asyncio
import os
import json

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

        consumer = await memphis.consumer(
            station_name="test_station",
            consumer_name="consumer",
        )

        messages: list[Message] = await consumer.fetch() # Type-hint the return here for LSP integration
        
        for consumed_message in messages:
            msg_data = json.loads(consumed_message.get_data())

            # Do something with the message data

            await consumed_message.ack()

    except Exception as e:
        print(e)
    finally:
        await memphis.close()

if __name__ == '__main__':
  asyncio.run(main()) 