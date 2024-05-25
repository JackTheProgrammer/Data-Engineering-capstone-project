# Import libraries required for connecting to mysql

# Import libraries required for connecting to DB2 or PostgreSql

# Connect to MySQL

# Connect to DB2 or PostgreSql

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

import psycopg2
import mysql.connector as msql_conn

# connectction details

dsn_hostname = '127.0.0.1'
dsn_user='postgres'
dsn_pwd ='ODYxMy1mYXdhZGF3'
dsn_port ="5432"
dsn_database ="postgres"


# create connection

conn = psycopg2.connect(
   database=dsn_database,
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

mysql_conn = msql_conn.connect(
    user='root', 
    password='MjEzMzQtZmF3YWRh',
    host='127.0.0.1',
    database='sales'
)

#Create a cursor object using cursor() method

cursor = conn.cursor()
mysql_cursor = mysql_conn.cursor()

def get_last_rowid():
    last_row_query = "SELECT max(rowid) FROM sales_data"
    cursor.execute(last_row_query)
    last_row_id = cursor.fetchall()[0]

    return last_row_id

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the 
# one on the Data warehouse. The function get_latest_records must return  
# a list of all records that have a rowid greater than the last_row_id 
# in the sales_data table in the sales database on the MySQL 
# staging data warehouse.

def get_latest_records(rowid):
    latest_rec_query = f"SELECT * FROM sales_data WHERE rowid > {rowid}"
    mysql_cursor.execute(latest_rec_query)
    records = mysql_cursor.fetchall()
    return records

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.
def insert_records(records):
	insert_query = "INSERT INTO sales_data(rowid, product_id, customer_id, price, quantity, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
    
    for record in records:
        cursor.execute(insert_query, record)
        
    conn.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
# disconnect from DB2 or PostgreSql data warehouse 

cursor.close()
conn.close()

mysql_conn.close()
mysql_cursor.close()
# End of program