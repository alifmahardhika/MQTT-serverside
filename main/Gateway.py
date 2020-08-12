import paho.mqtt.client as mqtt  # perlu pip install dulu
import json
import mariadb
from pathlib import Path

# MQTT Settings
MQTT_Broker = "app.itsmyhealth.id"
MQTT_Port = 1883
Keep_Alive_Interval = 60
MQTT_TOPIC_TEMPERATURE = "v1/devices/me/telemetry"
pub_complete = False

# MARIA DB Name
DB_HOST = ''
DB_USER = ''
DB_PASS = ''
DB_NAME = ''

# CLIENT-CREDENTIALS DICTIONARY; key = client name, value = usercredentials di thingsboard
client_credentials = {
    "Test": "credtest",  # kalo ada client baru tinggal tambah
}

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
        print("Connected with THINGSBOARD Broker: " + str(MQTT_Broker))


def on_publish(client, userdata, mid):
    global pub_complete
    pub_complete = True
    pass


def on_disconnect(client, userdata, rc):

    if rc != 0:
        pass


def getDBConfig():
    config_location = Path(__file__).absolute().parent
    file_location = config_location / 'dbconfig.txt'
    try:
        f = file_location.open()
        config = f.readline().split(',')
    except Exception as e:
        print(e)

    global DB_HOST
    global DB_USER
    global DB_PASS
    global DB_NAME
    DB_HOST = config[0]
    DB_USER = config[1]
    DB_PASS = config[2]
    DB_NAME = config[3]
    f.close()


def db_connect():
    mydb = mariadb.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    try:
        db_cursor = mydb.cursor()
        return db_cursor
    except mariadb.Error as e:
        print(e)
        return None


def get_client(mac_addr, db_cursor):
    if(len(mac_addr) != 17):
        raise Exception("invalid mac address")
    sql_query = "SELECT client FROM sensor_records WHERE sensor_mac_addr ='" + mac_addr + "'"
    db_cursor.execute(sql_query)
    try:
        client = db_cursor.fetchone()[0]
        return str(client)
    except Exception as e:
        print('Client not found')
        print(e)


def create_client(client):
    tb_credentials = client_credentials.get(client)
    mqttc = mqtt.Client('fromserver')
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_publish = on_publish
    # mqttc.username_pw_set(tb_credentials, None)
    mqttc.username_pw_set("Y1RmDk5xNj1C5KfyxRLG", None)

    return mqttc


def publish_to_thingsboard(mqttclient, message):
    try:
        mqttclient.publish(MQTT_TOPIC_TEMPERATURE, message, qos=1)
        return True
    except Exception as e:
        print(e)
        return False


def initial_processor(topic, message):
    print("CALLED INIT: " + topic + "\n" + message)
    global pub_complete
    getDBConfig()
    try:
        db_cursor = db_connect()
        if(db_cursor == None):
            raise Exception("Database connection failed")
    except Exception as e:
        print(e)
        return None
    mac_addr = topic.split("/")[3]
    client = get_client(mac_addr, db_cursor)
    mqtt_client = create_client(client)
    mqtt_client.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))
    publish_to_thingsboard(mqtt_client, message)
    print("pb: " + str(pub_complete))
    mqtt_client.loop_start()
    while(pub_complete == False):
        continue
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    pub_complete = False
    print("FINISHED")
    return True


# initial_processor('/sensor/v1/50:02:91:87:5e:3d',
#                   '{"sensor_mac_addr": "50:02:91:87:5e:3d", "time_stamp": "29-Jul-2020", "temperature": "36.1"}')
