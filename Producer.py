import pika
import json

# PATTERN DESIGN : STATE OF THE ART DESIGN

class MetaClass(type):

    _instance = {}

    def __call__(cls, *args, **kwargs):
        # Singelton Design Pattern
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]
        
class RabbitMQconfig(metaclass=MetaClass):

    def __init__(self, queue='hello', host= 'rabbitmq', routingKey= 'hello', exchange=''):
        # Configure RabbitMQ Server 
        self.queue = queue
        self.host = host
        self.routingKey = routingKey
        self.exchange = exchange        

class RabbitMQ(object):

    __slots__ = ["server", "_channel", "_connection"]

    def __init__(self, server):

        """
        :param server: Object of class RabbitMQconfig
        """
        self.server = server

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host)
        )
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.server.queue, durable=True)

    def __enter__(self):
        print("__enter__")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        print("__exit__")
        self._connection.close()

    def publish(self, payload = {}):

        """
        :param payload: JSON payload
        :return: None
        """
        
        self._channel.basic_publish(
            exchange=self.server.exchange,
            routing_key = self.server.routingKey,
            body=str(payload)
        )
        print(f"Published Message: {payload}")
        

