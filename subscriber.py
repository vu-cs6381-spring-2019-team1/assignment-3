from CS6381 import ToBroker
import sys

def message_handler1(topic, value):
    print("In message handler callback of \nTopic: {}, Value: {}".format(topic, value))

Broker_API = ToBroker()
topic1 = sys.argv[1]
Broker_API.register_sub("{}".format(topic1))
Broker_API.notify("{}".format(topic1), message_handler1)