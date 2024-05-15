import pika
import time

QUEUE_NAME = 'test'

class Publisher:

    channel = None
    conn = None

    def __init__(self):
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters('rabbitmq',
                                       5672,
                                       '/',
                                       credentials)
        try:
            self.conn = pika.BlockingConnection(parameters)
        except Exception as e:
            time.sleep(10)
            self.conn = pika.BlockingConnection(parameters)

        self.channel = self.conn.channel()
        self.channel.queue_declare(queue=QUEUE_NAME)


    
    def publish(self, message):
        self.channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message.to_json())
        print(" [x] Sent %r" % message)
    #     este publish lo llamare en los post del controllador, i lo que habia ahi lo eliminamos,el delete también lo podriamos enviar
    
    def close(self):
        self.conn.close()