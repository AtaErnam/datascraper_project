from Data_Scraping.App_Info_Publisher import RabbitMQServer, RabbitMQServerConfigure, MetaClass
import datetime
import sys, os, ast

import yaml
with open('config.yaml', 'r') as file:
   prime_service = yaml.safe_load(file)


if __name__ == "__main__":
    start = datetime.datetime.now()
    serverconfigure = RabbitMQServerConfigure(host=prime_service["rabbitmq"]["host"], queue="app.crawler.uid.queue", publishing_queue='app.crawler.app.metadata.queue', publishing_routingKey='', publishing_exchange='app.crawler.app.metadata',
                                                port=prime_service["rabbitmq"]["port"],virtual_host=prime_service["rabbitmq"]["virtual_host"],
                                                user=prime_service["rabbitmq"]["user"],passw=prime_service["rabbitmq"]["passw"])
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
