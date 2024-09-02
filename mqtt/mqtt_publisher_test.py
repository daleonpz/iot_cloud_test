import paho.mqtt.client as mqtt
import json
import random
import time


class MQTTMessagePublisher:
    """Publishes messages to an MQTT topic."""

    def __init__(self, mqtt_broker, mqtt_port, mqtt_topic):
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.mqtt_topic = mqtt_topic

        # Initialize MQTT client
        self.client = mqtt.Client()

    def connect(self):
        """Connects to the MQTT broker."""
        self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
        print(f"Connected to MQTT broker at {self.mqtt_broker}:{self.mqtt_port}")

    def publish(self, message):
        """Publishes a message to the MQTT topic."""
        self.client.publish(self.mqtt_topic, json.dumps(message))
        print(f"Published message to {self.mqtt_topic}: {message}")


class SensorDataSimulator:
    """Simulates sensor data."""

    @staticmethod
    def generate_data():
        """Generates random sensor data."""
        return {
            "temperature": random.randint(50, 100),
            "battery_level": random.randint(2000, 5000),
        }


class MQTTDataSender:
    """Sends sensor data to an MQTT topic at regular intervals."""

    def __init__(self, mqtt_publisher, simulator, interval=5):
        self.mqtt_publisher = mqtt_publisher
        self.simulator = simulator
        self.interval = interval

    def start_sending(self):
        """Starts sending data to the MQTT topic at regular intervals."""
        self.mqtt_publisher.connect()

        while True:
            data = self.simulator.generate_data()
            self.mqtt_publisher.publish(data)
            time.sleep(self.interval)


# Configuration
MQTT_BROKER = "localhost"  # usually 127.0.0.1
MQTT_PORT = 1883
MQTT_TOPIC = "test/dummy_topic"
PUBLISH_INTERVAL = 5  # Seconds

# Initialize the MQTT publisher and sensor simulator
mqtt_publisher = MQTTMessagePublisher(MQTT_BROKER, MQTT_PORT, MQTT_TOPIC)
sensor_simulator = SensorDataSimulator()

# Initialize the data sender and start sending messages
data_sender = MQTTDataSender(mqtt_publisher, sensor_simulator, PUBLISH_INTERVAL)
data_sender.start_sending()
