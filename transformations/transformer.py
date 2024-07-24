import paho.mqtt.client as mqtt
from cassandra.cluster import Cluster

def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    temp_c = (data['temperature'] - 32) * 5.0/9.0
    battery_percent = (data['battery_level'] / 5000.0) * 100

    query = "INSERT INTO measurements (id, temperature, battery_level) VALUES (uuid(), %s, %s)"
    session.execute(query, (temp_c, battery_percent))

client = mqtt.Client()
client.connect("mqtt", 1883, 60)
client.on_message = on_message
client.subscribe("iot/data")
client.loop_forever()
