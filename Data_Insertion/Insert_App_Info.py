# Consume App Info from exchange
# Map app categories
# Insert App Info and categories to DB
from Data_Scraping.SCRAPER_V2 import App
import pika
import json
import datetime
import pyodbc
import yaml

with open('config.yaml', 'r') as file:
   prime_service = yaml.safe_load(file)

from elasticsearch import Elasticsearch

es = Elasticsearch(hosts="http://localhost:9200",basic_auth=(prime_service["elastic"]["user"], prime_service["elastic"]["passw"]))

conn = pyodbc.connect(f'Driver={prime_service["database"]["driver"]};'
                      f'Server={prime_service["database"]["server"]};'
                      f'Database={prime_service["database"]["database"]};'
                      f'UID={prime_service["database"]["user"]};'
                      f'PWD={prime_service["database"]["passw"]};'
                      f'Trusted_Connection=yes;')

cursor = conn.cursor()

class MetaClass(type):

    _instance = {}

    def __call__(cls, *args, **kwargs):
        # Singelton Design Pattern
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]

class RabbitMQServerConfigure(metaclass=MetaClass):
    
    def __init__(self, host='rabbitmq-local', queue='hello',port='5672',virtual_host='/',user='guest',passw='guest'):

        # Server initialization

        self.host = host
        self.queue = queue
        self.port = port
        self.virtual_host = virtual_host
        self.user = user
        self.passw = passw
   

class RabbitMQServer():

    def __init__(self,server):

        """
        :param server: Object of class RabbitMQServerConfigure
        """

        self.server = server
        credentials = pika.PlainCredentials(self.server.user, self.server.passw)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host,port=self.server.port,virtual_host=self.server.virtual_host,credentials=credentials)
        )
        self._channel = self._connection.channel()
        self._temp = self._channel.queue_declare(queue=self.server.queue, durable=True)
        
    @staticmethod
    def callback_insert(ch,method, properties, body):

        Payload = body.decode("utf-8")
        
        print("Data Received: {}".format(Payload))

        if Payload != "Null":
            app=json.loads(Payload)
            app = App(**app)
            
            InsertAppCodes = ("INSERT INTO [TEST DB].dbo.ApplicationCodes(ApplicationName,UidName,InsertionDate,IconUrl,LastUpdateDate,IsActive,DetailUrl,IsSystemApp) VALUES (?,?,?,?,?,?,?,?)")
            TruncateErrorNeglect = ("SET ANSI_WARNINGS OFF")
            
            Values = [app.AppName,app.Uid,datetime.datetime.now(),app.IconUrl,datetime.datetime.now(),1,app.DetailUrl,"0"]      
            
            #Processing Query    
            cursor.execute(TruncateErrorNeglect)
            cursor.execute(InsertAppCodes,Values)     
            #Commiting any pending transaction to the database.    
            conn.commit()    
            #closing connection 

            try:
                print(es.index(index="app_data", document=app.__dict__))
            except AttributeError:
                pass
        else:
            pass
        
    def start_server_consume_insert(self):
        self._channel.basic_consume(
                queue=self.server.queue,
                on_message_callback=RabbitMQServer.callback_insert,
                auto_ack=True
            )
        print("[*] Waiting for messages to exit press CTRL + C")
        self._channel.start_consuming()