import json
from Extract_Uid.Producer import RabbitMQ, RabbitMQconfig, MetaClass
import pika
import datetime

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
    server = RabbitMQconfig(queue='test.queue', host=prime_service["rabbitmq"]["host"], routingKey='', exchange='test.exchange',
                                port=prime_service["rabbitmq"]["port"],virtual_host=prime_service["rabbitmq"]["virtual_host"],
                                user=prime_service["rabbitmq"]["user"],passw=prime_service["rabbitmq"]["passw"])

    with RabbitMQ(server) as rabbitmq:
        start = datetime.datetime.now()
        Uid_list = ",".join(Uid_list)
        rabbitmq.publish(payload=Uid_list)
        finish = datetime.datetime.now() - start
        print(finish)


