from Set_Send_Uid import RabbitMQServer, RabbitMQServerConfigure, MetaClass
import datetime
import sys, os, ast

import yaml
with open('config.yaml', 'r') as file:
   prime_service = yaml.safe_load(file)

if __name__ == "__main__":
    start = datetime.datetime.now()
    serverconfigure = RabbitMQServerConfigure(host=prime_service["rabbitmq"]["host"], queue='test.queue', publishing_queue='app.crawler.uid.queue', publishing_routingKey='', publishing_exchange='app.crawler.uid')
    server = RabbitMQServer(server=serverconfigure)

    try:
        server.start_server_for_consume_publish()
    except KeyboardInterrupt:
        print('Interrupted')
        finish = datetime.datetime.now() - start
        print(finish)
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
