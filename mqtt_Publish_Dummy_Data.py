import paho.mqtt.client as mqtt
import random
import threading
import json
from datetime import datetime

# ====================================================
# MQTT Settings
MQTT_Broker = "app.itsmyhealth.id"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic_Temperature = "Home/BedRoom/DHT22/Temperature"

# ====================================================


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        pass
        print("Unable to connect to MQTT Broker...")
    else:
        print("Connected with MQTT Broker: " + str(MQTT_Broker))


def on_publish(client, userdata, mid):
    pass


def on_disconnect(client, userdata, rc):
    if rc != 0:
        pass


def publish_To_Topic(topic, message):
    mqttc.publish(topic, message)
    print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))


# ====================================================
# FAKE SENSOR
# Dummy code used as Fake Sensor to publish some random values
# to MQTT Broker


def publish_Fake_Sensor_Values_to_MQTT():
    threading.Timer(3.0, publish_Fake_Sensor_Values_to_MQTT).start()
    Temperature_Fake_Value = float("{0:.2f}".format(random.uniform(1, 30)))

    Temperature_Data = {}
    Temperature_Data['Sensor_ID'] = "Dummy-2"
    Temperature_Data['Date'] = (
        datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
    Temperature_Data['Temperature'] = Temperature_Fake_Value
    temperature_json_data = json.dumps(Temperature_Data)

    print("Publishing fake Temperature Value: " +
          str(Temperature_Fake_Value) + "...")
    publish_To_Topic(MQTT_Topic_Temperature, temperature_json_data)


# ====================================================
# MAIN PROCESS
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.username_pw_set("sens1", "testing1")

print("connecting")
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))
publish_Fake_Sensor_Values_to_MQTT()

mqttc.loop_forever()
