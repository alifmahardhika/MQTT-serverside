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
DB_HOST = 'localhost'
DB_USER = 'alif'
DB_PASS = 'alif'
DB_NAME = 'iotdb'

# ===============================================================


# # Database Manager Class
# class DatabaseManager():

#     def __init__(self):

#         # self.conn.execute('pragma foreign_keys = on') ===========>>> perlu review buat apa dan kalau perlu maka equivalent di mariadb gimana commandnya
#         # self.conn.commit() ===============>>> previous transaction's terminator

#     def add_del_update_db_record(self, sql_query, args=()):
#         cur.execute(sql_query, args)
#         conn.commit()
#         return

#     def __del__(self):
#         cur.close()
#         conn.close()

# # ===============================================================
# # Functions to push Sensor Data into Database


# Function to save Temperature to DB Table
def Temp_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    sensor_mac_addr = json_Dict['sensor_mac_addr']
    time_stamp = json_Dict['time_stamp']
    temperature = json_Dict['temperature']
    insert_query = 'insert into temperature_records (sensor_mac_addr, time_stamp, temperature) values ("' + \
        str(sensor_mac_addr) + '", "' + str(time_stamp) + \
        '", ' + str(temperature) + ')'

    return insert_query


# # ===============================================================
# # Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
    # set db connection
    try:
        conn = mariadb.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        print("\n=========================Database connection to " +
              DB_HOST + " SUCCESS===================================\n")
    except mariadb.Error as e:
        print(e)
    cur = conn.cursor()
    try:
        cur.execute(Temp_Data_Handler(jsonData))
        print("INSERTED!\n")
    except Exception as e:
        print("FAILED")

    cur.close()
    conn.close()


#     if Topic == "Temperature":
#         Temp_Data_Handler(jsonData)
#         # insert other topic cases below
#     # elif Topic == "Home/BedRoom/DHT22/Humidity":
#     # 	DHT22_Humidity_Data_Handler(jsonData)

# # ===============================================================


# MAIN TEST PROCESS
# top = "Temperature"
# dat = '{"sensor_mac_addr": "dc:87", "temperature": 32.13}'
# sensor_Data_Handler(top, dat)
