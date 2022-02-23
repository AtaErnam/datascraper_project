# Consume Uid from exchange 
# Scrape with Uid
# Publish to App_Info exchange 

import datetime
from datetime import timedelta
import pika
from Producer import RabbitMQ, RabbitMQconfig, MetaClass
from SCRAPER_V2 import Scraper, App
import requests
from concurrent.futures import ThreadPoolExecutor
import json

import yaml
with open('config.yaml', 'r') as file:
   prime_service = yaml.safe_load(file)

callback_server = RabbitMQconfig(queue='app.crawler.uid.queue', host=prime_service["rabbitmq"]["host"], routingKey='', exchange='app.crawler.uid')

class MetaClass(type):

    _instance = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]

class RabbitMQServerConfigure(metaclass=MetaClass):
    
    def __init__(self, host='rabbitmq', queue='hello', publishing_queue='hello', publishing_routingKey='', publishing_exchange='hello'):

        # Server initialization

        self.host = host
        self.queue = queue

        self.publishing_queue = publishing_queue
        self.publishing_routingKey = publishing_routingKey
        self.publishing_exchange = publishing_exchange
        self.callback_server = RabbitMQconfig(queue=self.publishing_queue, host=self.host, routingKey=self.publishing_routingKey, exchange=self.publishing_exchange)
   
class RabbitMQServer():

    def __init__(self,server):

        """
        :param server: Object of class RabbitMQServerConfigure
        """

        self.server = server
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host)
        )
        self._channel = self._connection.channel()
        self._temp = self._channel.queue_declare(queue=self.server.queue, durable=True)

    @staticmethod
    def callback_publish(self,method, properties, body):

        Payload = body.decode("utf-8")
        
        print("Data Received: {}".format(Payload))
        
        Payload = Payload.split(",")

        Uid_list = set({})
        for i in Payload:
            Uid_list.add(i)
            
            if len(Uid_list) >= 250:
                
                with RabbitMQ(callback_server) as rabbitmq:
                    Uid_list = ",".join(Uid_list)
                    rabbitmq.publish(payload=Uid_list)
                Uid_list = set({})
           
    def start_server_for_consume_publish(self):
        self._channel.basic_consume(
                queue=self.server.queue,
                on_message_callback=RabbitMQServer.callback_publish,
                auto_ack=True
            )
        print("[*] Waiting for messages to exit press CTRL + C")
        self._channel.start_consuming()

