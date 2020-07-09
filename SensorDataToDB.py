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
DB_HOST = 'localhost'
DB_USER = 'alif'
DB_PASS = 'alif'
DB_NAME = 'iotdb'


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
1. Try untuk connect ke database dengan predefined credentials, output berhasil/tidak diprint di terminal yang menjalankan program listener
2. Initialize kursor database yang akan execute query
3. Try untuk coba execute sql query, output berhasil/ tidak diprint di terminal yang menjalankan program listener
4. Close connection database
TODO: Buat cases berdasarkan jenis topik
========================================================================================================================
'''


def sensor_data_handler(Topic, jsonData):
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
        cur.execute(temp_data_handler(jsonData))
        print("INSERTED!\n")
    except Exception as e:
        print("FAILED")

    cur.close()
    conn.close()

# MAIN TEST PROCESS (uncomment supaya bisa tes jalanin program ini aja)
# top = "Temperature"
# dat = '{"sensor_mac_addr": "dc:87", "temperature": 32.13}'
# sensor_data_handler(top, dat)
