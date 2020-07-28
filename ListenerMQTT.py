#!/usr/bin/env python3
'''
========================================================================================================================
NOTE: Program untuk menerima published mqtt messages. Adalah MQTT Client jadi basically bisa dijalanin dari mana aja 
tidak harus di execute diserver. 
NOTE: Execute di terminal manapun tidak harus di server.
NOTE: Tiap database maksimal hanya 1 program ini yang jalan, kalau lebih nanti ada database entry yang duplicate
NOTE: Requirements: pip install paho-mqtt
========================================================================================================================
'''
import paho.mqtt.client as mqtt  # perlu pip install dulu
from SensorDataToDB import sensor_data_handler


# MQTT Settings
MQTT_BROKER = "app.itsmyhealth.id"
MQTT_PORT = 1882
Keep_Alive_Interval = 45
MQTT_TOPIC = "Temperature"  # kalau ada multiple topics bisa pakai wildcard


'''
================================================================
Callback Functions, dipanggil sebagai hook dari client mqtt
================================================================
'''


# Checks Result Code. RC = 0 = successful connection, other = refused
def on_connect(mosq, obj, flags, rc):
    if rc != 0:
        pass
        print("Unable to connect to MQTT Broker...")
    else:
        print("Connected with MQTT Broker: " + str(MQTT_BROKER))
        # calls subscribe function, triggers on_subscribe callback
        mqttc.subscribe(MQTT_TOPIC, 0)


# ditrigger kalau ada message dengan topic yang di subscribe
# akan memanggil function data handler dari file SensorDataToDB.py
def on_message(mosq, obj, msg):
    print("MQTT Data Received...")
    print("MQTT Topic: " + msg.topic)
    print("Data: " + msg.payload.decode('utf-8'))
    sensor_data_handler(msg.topic, msg.payload)  # call function data handler


# dipakai buat debug
def on_subscribe(mosq, obj, mid, granted_qos):
    # print('called subs')
    pass


'''
================================================================
Main process yang diexecute:
1. configure mqtt client connection
2. assign event callbacks
3. connect to mqtt broker
4. start a forever loop

Program tidak akan diterminate kecuali ada interrupt dari system
================================================================
'''
print('start listener')
mqttc = mqtt.Client()
mqttc.username_pw_set("sens1", "testing1")

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe
print('Finished setup, connecting ...')

# Connect
mqttc.connect(MQTT_BROKER, int(MQTT_PORT), int(Keep_Alive_Interval))
print('Connected and subscribed to topic')

# Continue the network loop
mqttc.loop_forever()
