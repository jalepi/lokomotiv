import zmq
from typing import Iterable, List
import importlib
from pickle import loads, dumps

def work(pub: str, sub: str, topic: bytes) -> Iterable:
    _key = topic.decode('utf8')
    _worker_run = f'worker-run-{_key}'.encode('utf8')
    _worker_result = f'worker-result-{_key}'.encode('utf8')
    _worker_exception = f'worker-exception-{_key}'.encode('utf8')

    with zmq.Context() as context:
        with context.socket(zmq.SUB) as subscriber:
            subscriber.connect(sub)
            subscriber.subscribe(_worker_run)
            print(_worker_run)
            with context.socket(zmq.PUB) as publisher:
                publisher.connect(pub)

                while True:
                    id = b''
                    msg_parts = subscriber.recv_multipart()
                    print(msg_parts)
                    try:
                        [topic, id, *message] = msg_parts

                        if topic == _worker_run:
                            result = worker_run(message)
                            yield publisher.send_multipart([_worker_result, id, result])

                        else:
                            yield None

                    except Exception as ex:
                        yield publisher.send_multipart([_worker_exception, id, dumps(ex)])
                        print(ex)


def worker_run(message: List[bytes]) -> bytes:
    print('message')
    print(message)

    [module_bytes, function_bytes, *params_bytes] = message
    
    print('params')
    print(params_bytes)

    module_name = module_bytes.decode('utf8')

    module = importlib.import_module(module_name)

    params = [loads(param_bytes) for param_bytes in params_bytes]

    function_name = function_bytes.decode('utf8')

    function = module.__dict__[function_name]

    result = function(*params)

    return dumps(result)