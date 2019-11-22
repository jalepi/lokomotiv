import zmq
from typing import Iterable, List


def broker(pub: str, sub: str, topic: bytes) -> Iterable:
    with zmq.Context() as context:
        with context.socket(zmq.SUB) as subscriber:
            subscriber.bind(sub)
            subscriber.subscribe(topic)
            with context.socket(zmq.PUB) as publisher:
                publisher.bind(pub)
                while True:
                    msg_parts = subscriber.recv_multipart()
                    yield publisher.send_multipart(msg_parts)


def subscribe(sub: str, topic: bytes) -> Iterable[List[bytes]]:
    with zmq.Context() as context:
        with context.socket(zmq.SUB) as subscriber:
            subscriber.connect(sub)
            subscriber.subscribe(topic)
            while True:
                yield subscriber.recv_multipart()


def publish(pub: str, topic: bytes, messages: Iterable[List[bytes]]) -> Iterable:
    with zmq.Context() as context:
        with context.socket(zmq.PUB) as publisher:
            publisher.connect(pub)
            for msg_parts in messages:
                yield publisher.send_multipart([topic, *msg_parts])
