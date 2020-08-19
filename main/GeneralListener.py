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
import systemd.daemon
import paho.mqtt.client as mqtt  # perlu pip install dulu
from datetime import datetime, timedelta
from Gateway import initial_processor, proxy_fun
from CheckConnHandler import check_conn_processor


# MQTT Settings
MQTT_BROKER = "app.itsmyhealth.id"
MQTT_PORT = 1882
Keep_Alive_Interval = 45
# kalau ada multiple topics bisa pakai wildcard
MQTT_TOPIC = "/sensor/v1/#"  # /sensor/


'''
================================================================
Callback Functions, dipanggil sebagai hook dari client mqtt
================================================================
'''


# Checks Result Code. RC = 0 = successful connection, other = refused
def general_on_connect(mosq, obj, flags, rc):
    if rc != 0:
        pass
        serverdatetime = get_server_date_time()
        print("[" + serverdatetime + "] Unable to connect to MQTT Broker...")
    else:
        print("Connected with MQTT Broker: " + str(MQTT_BROKER))
        # calls subscribe function, triggers general_on_subscribe callback
        mqtt_general.subscribe(MQTT_TOPIC, 0)


# ditrigger kalau ada message dengan topic yang di subscribe
# akan memanggil function data handler dari file SensorDataToDB.py
def general_on_message(mosq, obj, msg):

    topic_type = msg.topic.split("/")[3]
    if(topic_type == "server-connection"):
        connection_checked = check_conn_processor(
            msg.topic, msg.payload.decode('utf-8'))
        if(connection_checked):
            # maksudnya buat destroy thread tapi gatau ngefek apa engga haha
            connection_checked = None
            pass
    elif(len(topic_type) == 17):
        # initial_processor(msg.topic, msg.payload.decode('utf-8')) << somehow tidak bisa dipanggil langsung, jadi mesti lewat proxy fun
        process_finished = proxy_fun(msg.topic, msg.payload.decode('utf-8'))
        if(process_finished):
            process_finished = None  # maksudnya buat destroy thread tapi gatau ngefek apa engga haha
            pass
    else:
        serverdatetime = get_server_date_time()
        print("[" + serverdatetime + "] Bad topic request type: " + topic_type)
    print("------------ON MESSAGE SEQUENCE FINSIHED----------")

    # v1/devices/me/telemetry
    # mqtt_general.publish("/server-response",
    #               '{"isConnected":true, "datetime" : ' + serverdatetime + '}')
    # print('{"isConnected":true, "datetime" : ' + serverdatetime + '}')

    # dipakai buat debug


def general_on_subscribe(mosq, obj, mid, granted_qos):
    # print('called subs')
    pass


def get_server_date_time():
    seven_hours_from_server = datetime.now() + timedelta(hours=7)
    serverdatetime = '{:%d:%m:%Y:%H:%M:%S:%f:}'.format(
        seven_hours_from_server)
    return serverdatetime


'''
================================================================
Main process
[INIT DAEMON HERE]
================================================================
'''
# print('start listener')
mqtt_general = mqtt.Client()
mqtt_general.username_pw_set("sens1", "testing1")
# Assign event callbacks
mqtt_general.on_message = general_on_message
mqtt_general.on_connect = general_on_connect
mqtt_general.on_subscribe = general_on_subscribe
print('Finished setup, connecting ...')

# Connect
mqtt_general.connect(MQTT_BROKER, int(MQTT_PORT), int(Keep_Alive_Interval))
print('Connected and subscribed to topic ' + MQTT_TOPIC)
systemd.daemon.notify('READY=1')
# +++++++++++++++++++++++++++++++++++++++++++++++++++
# Continue the network loop
mqtt_general.loop_forever()
