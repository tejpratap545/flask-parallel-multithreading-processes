__author__ = "tejpratap"


from flask import Flask
import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import time
from datetime import datetime
import threading

conn_string = os.environ.get("AZURE_SERVICE_BUS_CONNECTION_STRING")
queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]

app = Flask(__name__)


class AzureServiceBusClient:
    def __init__(self):
        self.client = ServiceBusClient.from_connection_string(conn_string)

        thread = threading.Thread(
            target=self._process_data_events, name="azure_service_bus_thread"
        )
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):

        with self.client.get_queue_receiver(queue_name) as receiver:
            for msg in receiver:
                print("start receive message")
                print(str(msg))
                receiver.complete_message(msg)
                print("msg received successfully")

                # receive continuous messages using the secondary thread along with running flask
                print(f"receiver thread {threading.current_thread()}")
                time.sleep(0.1)

    def send_message(self, msg):
        with self.client.get_queue_sender(queue_name) as sender:
            sender.send_messages(ServiceBusMessage(f"{msg} send on {datetime.now()}"))
            print("Send message is done.")
            print(
                f"sender thread {threading.current_thread()}"
            )  # send message  using the main thread


@app.route("/send_msg/<msg>")
def send_message(msg):
    service_bus_client.send_message(msg)
    return f" {msg} is successfully sent"


@app.route("/")
def welcome():
    return """<h1>Welcome to flask server<h1>"""


if __name__ == "__main__":
    service_bus_client = AzureServiceBusClient()

    print(threading.enumerate())
    print(threading.active_count())  # two threads are running
    print(threading.current_thread())  # see current main thread flask server

    app.run()
