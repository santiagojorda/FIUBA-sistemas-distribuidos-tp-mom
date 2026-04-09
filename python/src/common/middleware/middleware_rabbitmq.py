import pika
from .middleware import (
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError
)

MAX_MESSAGES_PER_WORKER = 1


class _RabbitMQBase:
    def __init__(self, host):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _cleanup_resources(self):
        if self.channel and self.channel.is_open:
            try:
                self.channel.close()
            except Exception:
                pass
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except Exception:
                pass
        self.channel = None
        self.connection = None

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error en el canal de RabbitMQ") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado") from e

    def close(self):
        try:
            self._cleanup_resources()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e


class MessageMiddlewareQueueRabbitMQ(_RabbitMQBase, MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        super().__init__(host)
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.basic_qos(prefetch_count=MAX_MESSAGES_PER_WORKER)

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message
            )
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida al enviar a la cola") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error de canal al enviar a la cola") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado al enviar") from e

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
        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error en el canal de RabbitMQ") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado") from e


class MessageMiddlewareExchangeRabbitMQ(_RabbitMQBase, MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self.routing_keys = routing_keys
        self.exchange_name = exchange_name
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message
                )
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida al publicar en el exchange") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error de canal al publicar en el exchange") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado al enviar") from e

    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            )

        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        try:
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
        except pika.exceptions.AMQPConnectionError as e:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ") from e
        except pika.exceptions.AMQPChannelError as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error en el canal de RabbitMQ") from e
        except Exception as e:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError("Error interno inesperado") from e