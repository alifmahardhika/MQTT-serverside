'''
=============================================================================================
Program inisialisasi database. Bisa dipakai untuk kebutuhan reset database
Credentials perlu disesuaikan dengan target database
=============================================================================================
'''
import mariadb


# =========INSERT CREDENTIALS HERE========
# for testing purpose:
DB_HOST = 'localhost'
DB_USER = 'alif'
DB_PASS = 'alif'
DB_NAME = 'iotdb'
# =========================================


db = None
# Try connection
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


# define database cursor, will be used to exxecute sql queries
executor = db.cursor()


# query all databases and check if any db named iotdb alreadyexists
executor.execute("SHOW DATABASES")
dbExisted = False
for a in executor:
    if(a[0] == DB_NAME):
        dbExisted = True


# Create database
print('Creating database ...')
if (dbExisted):
    print('Database with name \"' + DB_NAME +
          '\" already existed, try dropping the previous database of rename the database to ber created, then run this program again\n')
    quit()

# execute database creation
try:
    executor.execute("CREATE DATABASE iotdb")
    print('Database created')

except Exception:
    print(Exception)

# create tables
print("creating tables ...")

# update database connections
db = mariadb.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)
executor = db.cursor()


# build queries
del_if_tbl_exists = "DROP TABLE IF EXISTS temperature_records"
create_tbl = "CREATE TABLE temperature_records (id INT AUTO_INCREMENT PRIMARY KEY, sensor_mac_addr CHAR(17), time_stamp VARCHAR(27), temperature TINYINT)"

# execute queries
try:
    executor.execute(del_if_tbl_exists)
    executor.execute(create_tbl)
    print('Table created')
except mariadb.Error as e:
    print('table creation failed: {e}')
    quit()

# Close DB connection
executor.close()
db.close()
