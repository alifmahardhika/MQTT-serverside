import paho.mqtt.client as mqtt  # perlu pip install dulu
import json
import mariadb
from pathlib import Path
from time import sleep
from datetime import datetime, timedelta


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
con_flag = False
# CLIENT-CREDENTIALS DICTIONARY; key = client name, value = usercredentials di thingsboard
client_credentials = {
    "Test": "Y1RmDk5xNj1C5KfyxRLG",  # kalo ada client baru tinggal tambah
}

'''
================================================================
Callback Functions, dipanggil sebagai hook dari client mqtt
================================================================
'''


def get_gateway_client():
    return client_credentials


def on_connect(client, userdata, flags, rc):
    global con_flag

    if rc != 0:
        con_flag = False
        serverdatetime = get_server_date_time()
        pass
        print("[" + serverdatetime + "] Unable to connect to MQTT Broker...: ")
    else:
        con_flag = True
        print("Connected with THINGSBOARD Broker: " + str(MQTT_Broker))


def on_publish(client, userdata, mid):
    global pub_complete
    pub_complete = True
    pass


def on_disconnect(client, userdata, rc):
    global con_flag

    con_flag = False
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
    try:
        mydb = mariadb.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        db_cursor = mydb.cursor()
        return db_cursor
    except Exception as e:
        serverdatetime = get_server_date_time()
        print('[' + serverdatetime + '] ' + str(e))
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
        serverdatetime = get_server_date_time()
        print('[' + serverdatetime + '] Client not found: ' + str(mac_addr))
        print(e)
        return None


def create_client(client):
    tb_credentials = client_credentials.get(client)
    mqttc = mqtt.Client()
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_publish = on_publish
    mqttc.username_pw_set(tb_credentials, None)

    return mqttc


def publish_to_thingsboard(mqttclient, message):
    try:
        mqttclient.publish(MQTT_TOPIC_TEMPERATURE, message)
        return True
    except Exception as e:
        serverdatetime = get_server_date_time()
        print('[' + serverdatetime + ']' + str(e))
        return False


def initial_processor(topic, message):
    global pub_complete, con_flag
    getDBConfig()
    try:
        db_cursor = db_connect()
        if(db_cursor == None):
            serverdatetime = get_server_date_time()
            raise Exception('[' + str(serverdatetime) +
                            "] Database connection failed.")
    except Exception as e:
        print(e)
        return None
    mac_addr = topic.split("/")[3]
    client = get_client(mac_addr, db_cursor)
    if(client == None):
        serverdatetime = get_server_date_time()
        print('[' + str(serverdatetime)+'] Unregistered MAC ADDRESS: ' + mac_addr +
              '\nTerminating publish sequence.\n')
        return False
    mqtt_client = create_client(client)
    mqtt_client.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))
    # publish_to_thingsboard(mqtt_client, message)
    mqtt_client.loop_start()
    con_count = 0
    while(con_flag == False):
        con_count += 1
        print(con_count)
        sleep(0.05)

    publish_to_thingsboard(mqtt_client, message)
    while(pub_complete == False):
        continue
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    pub_complete = False
    print("Publish sequence FINISHED")
    return True


def get_server_date_time():
    seven_hours_from_server = datetime.now() + timedelta(hours=7)
    serverdatetime = '{:%d:%m:%Y:%H:%M:%S:%f:}'.format(
        seven_hours_from_server)
    return serverdatetime


def proxy_fun(topic, message):
    return initial_processor(topic, message)


# initial_processor('/sensor/v1/50:02:91:87:5e:3d',
#                   '{"sensor_mac_addr": "50:02:91:87:5e:3d", "time_stamp": "29-Jul-2020", "temperature": "36.1"}')
# mosquitto_pub -d -h "app.itsmyhealth.id" -p 1882 -t "/sensor/v1/50:02:91:87:5e:3d" -u "sens1" -P "testing1" -m "{"sensor_mac_addr":"50: 02: 91: 87: 5e: 3d", "time_stamp": "29-Jul-2020", "temperature": "36.3"}"
