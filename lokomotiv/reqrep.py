from typing import Callable, List, Iterable
import zmq
import multiprocessing
import contextlib

def recvsend(connect: str, message_handler: Callable[[List[bytes]], List[bytes]]) -> None:
    with zmq.Context() as context:
        with context.socket(zmq.REP) as rep:
            rep.connect(connect)
            while True:
                #print('recvsend.receiving')
                msg = rep.recv_multipart()
                #print('recvsend.received')

                #print('recvsend.handling')
                msg = message_handler(msg)
                #print('recvsend.handled')

                #print('recvsend.sending')
                rep.send_multipart(msg)
                #print('recvsend.sent')

def run_broker(frontend: str, backend: str) -> None:
    with zmq.Context() as context:
        with context.socket(zmq.ROUTER) as router:
            router.bind("tcp://*:5559")
            with context.socket(zmq.DEALER) as dealer:
                dealer.bind("tcp://*:5560")
                zmq.proxy(router, dealer)

def send(connect: str, message_list: Iterable[List[bytes]]) -> Iterable[List[bytes]]:
    with zmq.Context() as context:
        with context.socket(zmq.REQ) as req:
            req.connect(connect)
            for message in message_list:
                #print('send.sending')
                req.send_multipart(message)
                #print('send.sent')

                #print('send.receiving')
                res = req.recv_multipart()
                #print('send.received')
                yield res


