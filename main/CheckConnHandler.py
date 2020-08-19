import paho.mqtt.client as mqtt  # perlu pip install dulu
import json
import mariadb
from datetime import datetime, timedelta

# MQTT Settings
MQTT_Broker = "app.itsmyhealth.id"
MQTT_Port = 1882
Keep_Alive_Interval = 60
MQTT_TOPIC_TEMPERATURE = "v1/devices/me/telemetry"
pub_complete = False


'''
================================================================
Callback Functions, dipanggil sebagai hook dari client mqtt
================================================================
'''


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        pass
        print("Unable to connect to MQTT Broker...\n Error code: " + str(rc))
    else:
        # print("Connected AS CHECKER: " + str(MQTT_Broker))
        pass


def on_publish(client, userdata, mid):
    global pub_complete
    pub_complete = True
    pass


def on_disconnect(client, userdata, rc):
    if rc != 0:
        pass


def check_conn_processor(topic, message):
    global pub_complete

    seven_hours_from_server = datetime.now() + timedelta(hours=7)

    serverdatetime = '{:%d:%m:%Y:%H:%M:%S:%f:}'.format(seven_hours_from_server)

    mqttc = mqtt.Client("checker")
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_publish = on_publish
    mqttc.username_pw_set("sens1", "testing1")
    mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

    mqttc.publish("/server-response",
                  '{"isConnected":true, "datetime" : ' + serverdatetime + '}', qos=1)

    # print("pb: " + str(pub_complete))
    mqttc.loop_start()
    while(pub_complete == False):
        continue
    mqttc.loop_stop()
    mqttc.disconnect()
    pub_complete = False
    print("Connection Response Sent.")
    return True
