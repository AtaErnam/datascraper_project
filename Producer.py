import pika

class MetaClass(type):

    _instance = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]
        
class RabbitMQconfig(metaclass=MetaClass):

    def __init__(self, queue='hello', host= 'rabbitmq', routingKey= 'hello', exchange='',port='5672',virtual_host='/',user='guest',passw='guest'):
        # Configure RabbitMQ Server 
        self.queue = queue
        self.host = host
        self.routingKey = routingKey
        self.exchange = exchange     
        self.port=port
        self.virtual_host = virtual_host
        self.user = user
        self.passw = passw   

class RabbitMQ(object):

    __slots__ = ["server", "_channel", "_connection"]

    def __init__(self, server):

        """
        :param server: Object of class RabbitMQconfig
        """
        self.server = server
        credentials = pika.PlainCredentials(self.server.user, self.server.passw)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host,port=self.server.port,virtual_host=self.server.virtual_host,credentials=credentials)
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
        

