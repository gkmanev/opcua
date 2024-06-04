from paho.mqtt import client as mqtt_client

class MQTTClient:
    def __init__(self, broker, port):
        self.mqtt = mqtt_client.Client()
        self.broker = broker
        self.port = port

    async def connect_and_publish(self, topic, payload):
        def on_connect(client, userdata, flags, rc):
            print("Connected to MQTT Broker!")
        self.mqtt.on_connect = on_connect
        self.mqtt.connect(self.broker, self.port)
        self.mqtt.loop_start()
        self.mqtt.publish(topic, payload)
        self.mqtt.loop_stop()
        self.mqtt.disconnect()