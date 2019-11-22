import zmq
import multiprocessing
from typing import Iterable
from lokomotiv.sync import publish

pub = 'tcp://localhost:5559'
sub = 'tcp://localhost:5560'

blocks = [
    (b'key-1', b'lokomotiv.pipeline', b'task1'),
    (b'key-2', b'lokomotiv.pipeline', b'task2'),
    (b'key-3', b'lokomotiv.pipeline', b'task3'),
]

connections = [
    (b'key-1', b'key-2'),
    (b'key-2', b'key-3'),
]

def next_key(key, connections):
    for s, t in connections:
        if s == key:
            return t

def find_block(key, blocks):
    for k, m, f in blocks:
        if k == key:
            return (k, m, f)
            

def work(pub: str, sub: str, topic: bytes) -> Iterable:
    _key = topic.decode('utf8')
    _worker_run = f'worker-run-{_key}'.encode('utf8')
    _worker_result = f'worker-result-{_key}'.encode('utf8')
    _worker_exception = f'worker-exception-{_key}'.encode('utf8')

    with zmq.Context() as context:
        with context.socket(zmq.SUB) as subscriber:
            subscriber.connect(sub)
            subscriber.subscribe(_worker_result)
            subscriber.subscribe(_worker_exception)

            with context.socket(zmq.PUB) as publisher:
                publisher.connect(pub)

                while True:
                    id = b''
                    msg_parts = subscriber.recv_multipart()
                    print(msg_parts)
                    try:
                        [topic, id, result] = msg_parts

                        if topic == _worker_result:
                            key = next_key(id.decode('utf8'), connections)
                            k, m, f = find_block(key, blocks)

                            yield publisher.send_multipart([_worker_run, k, m, f, result])

                        elif topic == _worker_exception:
                            print(result)
                        else:
                            yield None

                    except Exception as ex:
                        print(ex)
                        yield None

def work_forever(pub: str, sub: str, topic: bytes) -> None:
    for res in work(pub=pub, sub=sub, topic=topic):
        print(res)

if __name__ == '__main__':
    p1 = multiprocessing.Process(target=work_forever, kwargs={'pub': pub, 'sub': sub, 'topic': topic})
    p1.start()