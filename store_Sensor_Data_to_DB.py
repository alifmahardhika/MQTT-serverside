# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------


import json
import mariadb


# MARIA DB Name
DB_Name = "iotdb.db"


# ===============================================================
# Database Manager Class
class DatabaseManager():
    def __init__(self):
        self.conn = mariadb.connect(DB_Name)
        # self.conn.execute('pragma foreign_keys = on') ===========>>> perlu review buat apa dan kalau perlu maka equivalent di mariadb gimana commandnya
        # self.conn.commit() ===============>>> previous transaction's terminator
        self.cur = self.conn.cursor()

    def add_del_update_db_record(self, sql_query, args=()):
        self.cur.execute(sql_query, args)
        self.conn.commit()
        return

    def __del__(self):
        self.cur.close()
        self.conn.close()

# ===============================================================
# Functions to push Sensor Data into Database


# Function to save Temperature to DB Table
def Temp_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    sensor_mac_addr = json_Dict['sensor_mac_addr']
    time_stamp = json_Dict['time_stamp']
    temperature = json_Dict['temperature']

    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into temperature_records (sensor_mac_addr, time_stamp, temperature) values (?,?,?)", [
                                   sensor_mac_addr, time_stamp, temperature])
    del dbObj
    print("Inserted Temperature Data into Database\n")


# ===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
    if Topic == "Temperature":
        Temp_Data_Handler(jsonData)
        # insert other topic cases below
    # elif Topic == "Home/BedRoom/DHT22/Humidity":
    # 	DHT22_Humidity_Data_Handler(jsonData)

# ===============================================================
