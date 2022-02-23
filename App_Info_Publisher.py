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

callback_server = RabbitMQconfig(queue='app.crawler.app.metadata.queue', host=prime_service["rabbitmq"]["host"], routingKey='', exchange='app.crawler.app.metadata')


from elasticsearch import Elasticsearch

es = Elasticsearch(hosts="http://localhost:9200")

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
        
        def fetch(session, Uid):
            with RabbitMQ(callback_server) as rabbitmq:
                
                search_param = {
                    "query": {
                        "terms": {
                            "Uid": [Uid] # finds Uids
                        }
                    }
                }
                es_uid = es.search(index="app_data", body=search_param)["hits"]["hits"][0]["_source"]["Uid"]
                
                if es_uid == Uid:
                    
                    es_date = es.search(index="app_data", body=search_param)["hits"]["hits"][0]["_source"]["InsertionDate"]
                    es_date = datetime.datetime.fromisoformat(es_date)
                    modified_date = es_date + timedelta(days=-30)
                    
                    if modified_date <= datetime.datetime.now():

                        app = Scraper.Scrape(session,Uid)
                        app_json = json.dumps(app.__dict__,default=str)
                        print(app_json)
                        rabbitmq.publish(payload=app_json)
                else:
                    app = Scraper.Scrape(session,Uid)
                    app_json = json.dumps(app.__dict__,default=str)
                    print(app_json)
                    rabbitmq.publish(payload=app_json)
                    
        Payload = Payload.split(",")
        
        with ThreadPoolExecutor(max_workers=prime_service["threads"]["worker_num"]) as executor:
            with requests.Session() as session:
                executor.map(fetch, [session] * len(Payload), Payload)
                executor.shutdown(wait=True)

    def start_server_for_consume_publish(self):
        self._channel.basic_consume(
                queue=self.server.queue,
                on_message_callback=RabbitMQServer.callback_publish,
                auto_ack=True
            )
        print("[*] Waiting for messages to exit press CTRL + C")
        self._channel.start_consuming()

