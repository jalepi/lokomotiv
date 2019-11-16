import zmq

with zmq.Context() as context:
    with context.socket(zmq.PUB) as pub, context.socket(zmq.SUB) as sub:
        pub.bind("tcp://*:5560")
        sub.bind("tcp://*:5559")
        sub.subscribe('')
        while True:
            msg_parts = sub.recv_multipart()
            print(f'received {msg_parts}')

            pub.send_multipart(msg_parts)
            print(f'sent {msg_parts}')


        