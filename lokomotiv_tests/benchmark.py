from typing import List

import zmq
import time
import unittest
import multiprocessing

import lokomotiv.amq
import lokomotiv.reqrep

payload = [b'some message to send']

def simply_reply(connect):
    return lokomotiv.reqrep.recvsend(connect, lambda msg: msg)

def execute_reqrep():
    broker_process = multiprocessing.Process(target=lokomotiv.reqrep.run_broker, kwargs={'frontend': 'tcp://*:5559', 'backend': 'tcp://*:5560'})
    worker_process = multiprocessing.Process(target=simply_reply, kwargs={'connect': 'tcp://localhost:5560'})

    try:
        broker_process.start()
        worker_process.start()

        t0 = time.time()
        result = [m for m in lokomotiv.reqrep.send(connect='tcp://localhost:5559', message_list=[payload])]
        t1 = time.time()

        print(f'execute_reqrep took {t1 - t0}')

    finally:
        broker_process.terminate()
        worker_process.terminate()


def pubsub_broker():
    with zmq.Context() as context:
        with context.socket(zmq.PUB) as publisher, context.socket(zmq.SUB) as subscriber:
            publisher.bind('tcp://*:5560')
            subscriber.bind('tcp://*:5559')
            subscriber.subscribe(b'')
            while True:
                print('broker receiving')
                message = subscriber.recv_multipart()
                print('broker received')

                print('broker sending')
                publisher.send_multipart(message)
                print('broker sent')

def produce_messages():
     with zmq.Context() as context:
        with context.socket(zmq.PUB) as publisher:
            publisher.connect('tcp://localhost:5559')
            for content in payload:
                print('producer sending')
                publisher.send_multipart([b'process', content])
                print('producer sent')

def pubsub_worker():
    with zmq.Context() as context:
        with context.socket(zmq.PUB) as publisher, context.socket(zmq.SUB) as subscriber:
            publisher.connect('tcp://localhost:5559')
            subscriber.connect('tcp://localhost:5560')
            subscriber.subscribe(b'process')
            while True:
                [topic, content] = subscriber.recv_multipart()
                publisher.send_multipart([b'result', content])

def execute_pubsub():
    broker_process = multiprocessing.Process(target=pubsub_broker)
    worker_process = multiprocessing.Process(target=pubsub_worker)
    producer_process = multiprocessing.Process(target=produce_messages)

    try:
        broker_process.start()
        worker_process.start()

        count = 100
        with zmq.Context() as context:
            with context.socket(zmq.SUB) as subscriber:
                subscriber.connect('tcp://localhost:5560')
                subscriber.subscribe(b'result')

                producer_process.start()
                t0 = time.time()
                while count > 0:
                    [topic, content] = subscriber.recv_multipart()
                    print(content)
                    count = count -1
                t1 = time.time()
                print(f'execute_pubsub took {t1 - t0}')

    finally:
        broker_process.terminate()
        worker_process.terminate()

if __name__ == '__main__':
    execute_reqrep()

    execute_pubsub()
