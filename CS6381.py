import time
import zmq
import hashlib
import random
import os
import configparser
from threading import Thread, Lock
from queue import Queue

class ToBroker:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.ip = config['IP']
        self.ports = config['PORT']

        context = zmq.Context()

        self.pub_socket = context.socket(zmq.DEALER)
        self.pub_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['RECEIVE']))
        self.sub_socket = context.socket(zmq.DEALER)
        self.sub_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['SEND']))
        self.reqp_socket = context.socket(zmq.DEALER)
        self.reqp_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['REGISTER_PUBLISHER']))
        self.reqs_socket = context.socket(zmq.DEALER)
        self.reqs_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['REGISTER_SUBSCRIBER']))

        self.heartbeat_socket = context.socket(zmq.DEALER)
        self.heartbeat_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['HEARTBEAT']))

        self.pub_id = ""
        self.sub_id = ""

    def register_pub(self, topic):
        hash_obj = hashlib.md5()
        hash_obj.update("{}{}".format(random.randint(1, 99999), os.getpid()).encode())
        self.pub_id = hash_obj.hexdigest()
        self.reqp_socket.send_string("{}-{}".format(topic, self.pub_id))
        heartbeat_thread = Thread(target=self.heartbeat, args=(self.pub_id,))
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        return_msg = self.reqp_socket.recv()
        return_msg = return_msg.decode('utf-8')
        print("Return message: {}".format(return_msg))

    def register_sub(self, topic):
        hash_obj = hashlib.md5()
        hash_obj.update("{}{}".format(random.randint(1, 99999), os.getpid()).encode())
        self.sub_id = hash_obj.hexdigest()
        self.reqs_socket.send_string("{}-{}".format(topic, self.sub_id))
        heartbeat_thread = Thread(target=self.heartbeat, args=(self.sub_id,))
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        return_msg = self.reqs_socket.recv()
        return_msg = return_msg.decode('utf-8')
        print("Return message: {}".format(return_msg))

    def publish(self, topic, value):
        top_val = "{},{}-{}:{}".format(self.pub_id, topic, value, time.time())
        print("Publishing {}".format(top_val))
        self.pub_socket.send_string(top_val)

    def publish_helper(self):
        while True:
            msg_from_broker = self.pub_socket.recv_string()
            print(msg_from_broker)
            time.sleep(5)

    def notify(self, topic, callback_func):

        top_id = "{},{}".format(self.sub_id, topic)
        self.sub_socket.send_string(top_id)
        self.sub_socket.setsockopt(zmq.RCVTIMEO, 5000)
        
    def heartbeat(self, identity):
        while True:
            self.heartbeat_socket.send_string("{}".format(identity))
            time.sleep(10)

class FromBroker:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        ports = config['PORT']
        broker_context = zmq.Context()
        self.pub_reg_socket = broker_context.socket(zmq.ROUTER)   
        self.pub_reg_socket.bind("tcp://*:{}".format(ports['REGISTER_PUBLISHER']))
        self.sub_reg_socket = broker_context.socket(zmq.ROUTER)
        self.sub_reg_socket.bind("tcp://*:{}".format(ports['REGISTER_SUBSCRIBER']))
        self.sender_socket = broker_context.socket(zmq.ROUTER)
        self.sender_socket.bind("tcp://*:{}".format(ports['SEND']))
        self.receiver_socket = broker_context.socket(zmq.ROUTER)
        self.receiver_socket.bind("tcp://*:{}".format(ports['RECEIVE']))
        self.heartbeat_socket = broker_context.socket(zmq.ROUTER)
        self.heartbeat_socket.bind("tcp://*:{}".format(ports['HEARTBEAT']))
        self.discovery = {"publishers": {}, "subscribers": {}, "id": {}}
        self.queue = Queue()

    def register_publisher_handler(self):

        while True:
            print("Register Publisher waiting for message")
            address, top_id = self.pub_reg_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            topic, pub_id = top_id.split('-')
            self.discovery["publishers"][pub_id] = {"address": address, "topic": topic}
            self.discovery["id"][pub_id] = time.time()
            self.pub_reg_socket.send_multipart(
                [address, "{} is successfully registered".format(pub_id).encode('utf-8')])
            time.sleep(3)

    def register_subscriber_handler(self):
        while True:
            print("Register Subscriber waiting for message")
            address, top_id = self.sub_reg_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            topic, sub_id = top_id.split('-')

            self.discovery["subscribers"][sub_id] = {"address": address, "topic": topic}
            self.discovery["id"][sub_id] = time.time()
            print(self.discovery)
            self.sub_reg_socket.send_multipart(
                [address, "{} is successfully registered".format(address).encode('utf-8')])
            time.sleep(3)

    def update_subscriber_address(self):

        while True:
            address, top_id = self.sender_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            sub_id, topic = top_id.split(',')
            if sub_id in self.discovery["subscribers"].keys() and topic in self.discovery["subscribers"][sub_id]["topic"]:
                self.discovery["subscribers"][sub_id]["address"] = address

    def is_client_dead(self):
        while True:
            if self.discovery["id"]:
                delete_list = []
                lock = Lock()
                lock.acquire()
                try:
                    for identity in self.discovery["id"].keys():
                        last_heartbeat = self.discovery["id"][identity]
                        time_diff = int(round(time.time()-last_heartbeat))
                        threshold = 60 + 10*(len(self.discovery["id"]))
                        if time_diff > threshold:
                            delete_list.append(identity)
                finally:
                    lock.release()
                for identity in delete_list:
                    try:
                        self.discovery["publishers"].pop(identity)
                    except KeyError:
                        pass
                    try:
                        self.discovery["subscribers"].pop(identity)
                    except KeyError:
                        pass
                    try:
                        self.discovery["id"].pop(identity)
                    except KeyError:
                        pass
                    print("{} is dead!".format(identity))

            else:
                print("There's is no client")
                time.sleep(5)

    def update_heartbeat(self):
            address, heartbeat_id = self.heartbeat_socket.recv_multipart()
            heartbeat_recv_time = time.time()
            heartbeat_id = heartbeat_id.decode('utf-8')
            if heartbeat_id in self.discovery["id"].keys():
                self.discovery["id"][heartbeat_id] = heartbeat_recv_time

    def receive_handler(self):
        while True:
            address, msg = self.receiver_socket.recv_multipart()
            msg = msg.decode('utf-8')
            pub_id, top_val = msg.split(',')
            print("Received message: {}".format(msg))
            if pub_id not in self.discovery["publishers"].keys():
                print("Publisher isn't registered")
                self.receiver_socket.send_multipart([address, b"Publisher isn't registered. Register before publishing"])
            else:
                self.discovery["publishers"][pub_id]["address"] = address
                self.queue.put(top_val)

    def send_handler(self):
        while True:
            if self.discovery["subscribers"]:
                top_val = self.queue.get()
                topic, val = top_val.split('-')

                for sub_id in self.discovery["subscribers"].keys():
                    if topic in self.discovery["subscribers"][sub_id]["topic"]:
                        address = self.discovery["subscribers"][sub_id]["address"]
                        self.sender_socket.send_multipart([address, top_val.encode('utf-8')])
            else:
                print("No subscribers yet")
                time.sleep(5)
                continue

    def run(self):

        t1 = Thread(target=self.register_publisher_handler)
        t2 = Thread(target=self.register_subscriber_handler)
        t3 = Thread(target=self.receive_handler)
        t4 = Thread(target=self.send_handler)
        t5 = Thread(target=self.update_subscriber_address)
        t6 = Thread(target=self.is_client_dead)
        t7 = Thread(target=self.update_heartbeat)
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        t6.start()
        t7.start()
        while True:
            print("...")
            time.sleep(7)
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()
        t6.join()
        t7.join()



