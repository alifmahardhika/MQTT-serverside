'''
========================================================================================================================
NOTE: Program untuk menyimpan data yang diterima listener ke database. 
NOTE: Credentials database perlu disesuaikan.
NOTE: Diexecute oleh ListenerMQTT.py
NOTE: Requirement: pip install mariadb 
========================================================================================================================
'''

import json
import mariadb


# MARIA DB Name
DB_HOST = ''
DB_USER = ''
DB_PASS = ''
DB_NAME = ''


'''
==================================================
get db credentials dari file db config  
==================================================
'''


def getDBConfig():
    f = open("dbconfig.txt", "r")
    config = f.readline().split(',')
    global DB_HOST
    global DB_USER
    global DB_PASS
    global DB_NAME
    DB_HOST = config[0]
    DB_USER = config[1]
    DB_PASS = config[2]
    DB_NAME = config[3]
    f.close()


'''
========================================================================================================================
Function untuk parse json data dari listener, kemudian menyusun sql query untuk diexecute (disimpan ke database)
TODO: karena metode susun payloadnya concatenate, adaa sql injection vulnerability. perlu buat validasi data json, 
========================================================================================================================
'''


def temp_data_handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    sensor_mac_addr = json_Dict['sensor_mac_addr']
    time_stamp = json_Dict['time_stamp']
    temperature = json_Dict['temperature']
    insert_query = 'insert into temperature_records (sensor_mac_addr, time_stamp, temperature) values ("' + \
        str(sensor_mac_addr) + '", "' + str(time_stamp) + \
        '", ' + str(temperature) + ')'

    return insert_query


'''
========================================================================================================================
Main Function yang di panggil program listener. Proses:
1. Try untuk connect ke database dengan credentials, output berhasil/tidak diprint di terminal yang menjalankan program listener
2. Initialize kursor database yang akan execute query
3. Try untuk coba execute sql query, output berhasil/ tidak diprint di terminal yang menjalankan program listener
4. Close connection database
TODO: Buat cases berdasarkan jenis topik
========================================================================================================================
'''


def sensor_data_handler(Topic, jsonData):
    # set db connection
    getDBConfig()
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
        cur.execute(temp_data_handler(jsonData))
        print("INSERTED!\n")
    except Exception as e:
        print(e)

    cur.close()
    conn.close()


# MAIN TEST PROCESS(uncomment supaya bisa tes jalanin program ini aja)
# top = "Temperature"
# dat = '{"sensor_mac_addr": "dc:53:60:d8:77:32", "time_stamp": "10-Jul-2020 10:13:55:812105", "temperature": 38.58}'
# sensor_data_handler(top, dat)
