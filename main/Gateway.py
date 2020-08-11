import paho.mqtt.client as mqtt  # perlu pip install dulu
import json
import mariadb


# MARIA DB Name
DB_HOST = ''
DB_USER = ''
DB_PASS = ''
DB_NAME = ''


def getDBConfig():
    print("hoe")
    try:
        f = open("../dbconfig.txt", "r")
        config = f.readline().split(',')
    except Exception as e:
        print(e)
    print("yea")

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
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    try:
        db_cursor = mydb.cursor()
        return db_cursor
    except mariadb.Error as e:
        print(e)
        return None


def initial_processor(topic, message):
    getDBConfig()
    # try:
    #     db_cursor = db_connect()
    #     if(db_cursor == None):
    #         raise Exception("Database connection failed")
    # except Exception as e:
    #     print(e)
    #     return None
    mac_addr = topic.split("/")[2]
    # if(len(mac_addr) != 17):
    #     raise Exception("invalid mac address")
    # sql_query = "SELECT client FROM sensor_records WHERE sensor_mac_addr = '" + mac_addr + "'"
    # db_cursor.execute(sql_query)
    # client = db_cursor.fetchone()
    return("topic: " + topic + "\nmessage: " + message)
