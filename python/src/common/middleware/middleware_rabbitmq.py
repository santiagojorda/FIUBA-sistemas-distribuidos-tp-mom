import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

MAX_MESSAGES_PER_WORKER = 1

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.basic_qos(prefetch_count=MAX_MESSAGES_PER_WORKER)

    def send(self, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name, 
            body=message
        )

    def start_consuming(self, on_message_callback):
        
        def internal_callback(ch, method, properties, body):
            on_message_callback(
                body, 
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            )

        self.channel.basic_consume(
            queue=self.queue_name, 
            on_message_callback=internal_callback,
            auto_ack=False
        )
        self.channel.start_consuming()

    def stop_consuming(self):
        if self.channel:
            self.channel.stop_consuming()

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.routing_keys = routing_keys
        self.exchange_name = exchange_name
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        
    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            on_message_callback(
                body, 
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            )

        result = self.channel.queue_declare(
            queue='', 
            exclusive=True
        )
        queue_name = result.method.queue

        for routing_key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=queue_name,
                routing_key=routing_key
            )
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=internal_callback,
            auto_ack=False
        )
        self.channel.start_consuming()
    
    def stop_consuming(self):
        if self.channel:
            self.channel.stop_consuming()
    
    def send(self, message):
        for routing_key in self.routing_keys:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message
            )  
    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()