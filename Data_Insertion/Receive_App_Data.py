import sys, os
from Data_Insertion.Insert_App_Info import RabbitMQServer, RabbitMQServerConfigure, MetaClass

import yaml
with open('config.yaml', 'r') as file:
   prime_service = yaml.safe_load(file)

if __name__ == "__main__":
    serverconfigure = RabbitMQServerConfigure(host=prime_service["rabbitmq"]["host"], queue="app.crawler.app.metadata.queue",
                                            port=prime_service["rabbitmq"]["port"],virtual_host=prime_service["rabbitmq"]["virtual_host"],
                                            user=prime_service["rabbitmq"]["user"],passw=prime_service["rabbitmq"]["passw"])
    server = RabbitMQServer(server=serverconfigure)

    try:
        server.start_server_consume_insert()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)