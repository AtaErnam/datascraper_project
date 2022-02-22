import json
from Producer import RabbitMQ, RabbitMQconfig, MetaClass
import pika
import datetime

"""
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
Uid_channel = connection.channel()

Uid_channel.queue_declare(queue='test.queue', durable=True)

def Publish(Payload):
    Uid_channel.basic_publish(exchange='test.exchange',routing_key='',body=Payload)

urls = ""
url_list = []
while urls != "STOP":
    urls = input("Please enter as many urls a you want, enter STOP to stop: ")
    [url_list.append(urls) if urls != "STOP" else print("")]
    [Publish(urls) if urls != "STOP" else print("Stopped...")]

Uid_channel.close()
"""


import yaml
with open('config.yaml', 'r') as file:
   prime_service = yaml.safe_load(file)

with open("Uid_Names.json") as json_file:
    data = json.load(json_file)
    temp = data["Uid Names"]
    Uid_list = set({})
    for i in temp:
        Uid_list.add(list(i.values())[0])

if __name__ == "__main__":
    server = RabbitMQconfig(queue='test.queue', host=prime_service["rabbitmq"]["host"], routingKey='', exchange='test.exchange')

    with RabbitMQ(server) as rabbitmq:
        start = datetime.datetime.now()
        Uid_list = ",".join(Uid_list)
        rabbitmq.publish(payload=Uid_list)
        finish = datetime.datetime.now() - start
        print(finish)



"""
import yaml
with open('config.yaml', 'r') as file:
   prime_service = yaml.safe_load(file)

server = RabbitMQconfig(queue='app.crawler.uid.queue', host=prime_service["rabbitmq"]["host"], routingKey='', exchange='app.crawler.uid')

with open("Uid_Names.json") as json_file:
    data = json.load(json_file)
    temp = data["Uid Names"]
    Uid_list = set({})
    for i in temp:
        Uid_list.add(list(i.values())[0])

        if len(Uid_list) >= 250:
            with RabbitMQ(server) as rabbitmq:
                start = datetime.datetime.now()
                Uid_list = ",".join(Uid_list)
                rabbitmq.publish(payload=Uid_list)
                finish = datetime.datetime.now() - start
                print(finish)
            Uid_list = set({})
"""