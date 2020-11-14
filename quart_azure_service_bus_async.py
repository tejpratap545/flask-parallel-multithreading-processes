__author__ = "tejpratap"

import asyncio

from quart import Quart
import os
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient
import time
from datetime import datetime
import threading

conn_string = os.environ.get("AZURE_SERVICE_BUS_CONNECTION_STRING")
queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]

app = Quart(__name__)


class AzureServiceBusClient:
    def __init__(self):
        self.client = ServiceBusClient.from_connection_string(conn_string)

        thread = threading.Thread(
            target=self._callback,
            name="azure_service_bus_thread",
        )
        thread.setDaemon(True)
        thread.start()

    def _callback(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(self._process_data_events())
        loop.close()

    async def _process_data_events(self):

        async with self.client.get_queue_receiver(queue_name) as receiver:
            received_msgs = await receiver.receive_messages(
                max_message_count=10, max_wait_time=5
            )  # receive 10 messages at one time
            print("start receive message")
            for msg in received_msgs:

                print(str(msg))
                await receiver.complete_message(msg)

                # receive continuous messages using the secondary thread along with running flask
            print(f"receiver thread {threading.current_thread()}")
            print("msg received successfully")

            await asyncio.sleep(0.2)  # wait for 0.2 seconds ans restart receiving
            await self._process_data_events()

    async def send_message(self, msg):

        async with self.client.get_queue_sender(queue_name) as sender:
            await sender.send_messages(
                ServiceBusMessage(f"{msg} send on {datetime.now()}")
            )
            print("Send message is done.")
            print(
                f"sender thread {threading.current_thread()}"
            )  # send message  using the main thread


@app.route("/send_msg/<msg>")
async def send_message(msg):
    asyncio.create_task(
        service_bus_client.send_message(msg)
    )  # return response  without waiting for  send message
    return f" {msg} is successfully sent"


@app.route("/")
async def welcome():
    return """<h1>Welcome to flask server<h1>"""


if __name__ == "__main__":
    service_bus_client = AzureServiceBusClient()

    print(threading.enumerate())
    print(threading.active_count())  # two threads are running
    print(threading.current_thread())  # see current main thread flask server

    app.run(port=5002)
