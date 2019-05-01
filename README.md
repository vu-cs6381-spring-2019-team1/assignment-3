# ZMQ PubSub
1. publish(topic, value): Once registered with a topic, publish values on the topic
2. register_sub(topic): Registers the subscriber with the broker for a topic
3. register_pub(topic): RegisterS the publisher with the broker for a topic
4. notify(topic): Calls a callback object when matching topic is found

#### Broker:
1. Register requests from publishers and subscribers.
2. Handle incoming messages and acts as a discovery service and relay.
3. Handles heartbeat messages and manages dead nodes.

## Installation
install dependencies:
```
pip3 install -r requirements.txt
```

## Execution
```python3 broker.py```
```python3 publisher.py <topicname>```
```python3 subscriber.py <topicname>```

config.ini: The configuration is read from this file. You may change IP address and ports depending upon the machine your broker is running.