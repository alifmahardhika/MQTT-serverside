# pip install mysql-connector-python <<<<<<<<<<<<------------ INSTALL MYSQL CONNECTOR FIRST (bash)

import mariadb

DB_HOST = 'localhost'
DB_USER = 'alif'
DB_PASS = 'alif'
DB_NAME = 'iotdb'
db = None
try:
    db = mariadb.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS
    )
    print("\n=========================Database connection to " +
          DB_HOST + " SUCCESS===================================\n")
except Exception:
    print("\n=========================Database connection to " +
          DB_HOST + " FAILED===================================\n")
    quit()

# define database cursor
executor = db.cursor()
# query all databases and check if any db named iotdb alreadyexists
executor.execute("SHOW DATABASES")
dbExisted = False
for a in executor:
    if(a[0] == DB_NAME):
        dbExisted = True


# create database
print('Creating database ...')
if (dbExisted):
    print('Database with name \"' + DB_NAME +
          '\" already existed, try dropping the previous database of rename the database to ber created, then run this program again\n')
    quit()

try:
    executor.execute("CREATE DATABASE iotdb")
    print('Database created')

except Exception:
    print(Exception)

# create tables
print("creating tables ...")

db = mariadb.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)
executor = db.cursor()


del_if_tbl_exists = "DROP TABLE IF EXISTS Temperature_Records"
create_tbl = "CREATE TABLE temperature_records (id INT AUTO_INCREMENT PRIMARY KEY, sensor_mac_addr CHAR(17), time_stamp VARCHAR(20), temperature TINYINT)"


try:
    executor.execute(del_if_tbl_exists)
    executor.execute(create_tbl)
    print('Table created')
except mariadb.Error as e:

    print('table creation failed: {e}')
    quit()

# Close DB
executor.close()
db.close()
