from CS6381 import ToBroker
import random
import time
import sys

Broker_API = ToBroker()
topic1 = sys.argv[1]
Broker_API.register_pub("{}".format(topic1))
while True:
    Broker_API.publish("{}".format(topic1), random.randint(1, 1000))
    time.sleep(5)
