'''
========================================================================================================================
Program untuk simulasi mqtt message publishing. Dipakai untuk testing. Credentials database perlu disesuaikan.
Tidak perlu diexecute
Requirements: pip install paho-mqtt, pip install getmac 
========================================================================================================================
'''
import paho.mqtt.client as mqtt
import random
import threading
import json
from getmac import get_mac_address as gma  # simulate mac address
from datetime import datetime
from math import floor

# MQTT Settings
MQTT_Broker = "app.itsmyhealth.id"
MQTT_Port = 1882
Keep_Alive_Interval = 45
MQTT_TOPIC_TEMPERATURE = "Temperature"


'''
================================================================
Callback Functions, dipanggil sebagai hook dari client mqtt
================================================================
'''


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


'''
================================================================
Program Functions
================================================================
'''


# Publisher topic dan payload
def publish_to_topic(topic, message):
    mqttc.publish(topic, message)
    print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))


# Dummy data untuk testing. Struktur payload di publisher asli HARUS sama dengan struktur payload disini
def dummy_payload_builder():
    threading.Timer(3.0, dummy_payload_builder).start()
    temperature_dummy_value = float("{0:.2f}".format(random.uniform(33, 39)))

    payload = {}
    mac = gma()[0:-2]
    # dummy mac addresses (5 different mac address)
    mac += str(floor(random.uniform(10, 15)))
    payload['sensor_mac_addr'] = mac
    payload['time_stamp'] = (
        datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
    payload['temperature'] = temperature_dummy_value
    payload_json_data = json.dumps(payload)

    print("Publishing dummy Temperature Value: " +
          str(temperature_dummy_value) + "...")
    publish_to_topic(MQTT_TOPIC_TEMPERATURE, payload_json_data)


'''
================================================================
Main process, diexecute saat program dijalankan lewat terminal
================================================================
'''
# Set konfigurasi mqtt client dan credentials
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.username_pw_set("sens1", "testing1")

# connect ke mqtt broker
print("connecting")
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# build and publish dummy mqtt messages, akan diterima ListenerMQTT.py
dummy_payload_builder()

# PENTING: somehow kalau loop mqtt clientnya gak forever clientnya gamau connect
mqttc.loop_forever()
