import zmq
import time

with zmq.Context() as context:
    with context.socket(zmq.PUB) as pub:
        pub.connect('tcp://localhost:5559')
        while True:
            fun_message = [b'fun', b'hi there']
            print(f'sending {fun_message}')

            pub.send_multipart(fun_message)

            sad_message = [b'sad', b'so sad']
            print(f'sending {sad_message}')
            pub.send_multipart(sad_message)
            
            time.sleep(1)