import zmq

with zmq.Context() as context:
    with context.socket(zmq.SUB) as sub:
        sub.connect('tcp://localhost:5560')
        sub.subscribe(b'fun')

        while True:
            msg_parts = sub.recv_multipart()
            print(f'received {msg_parts}')