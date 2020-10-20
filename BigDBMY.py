from sklearn.model_selection import train_test_split
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import pyodbc 
import mysql.connector
import time

from sklearn.datasets import load_iris

fields = ['Complaint Type', 'Borough', 'Agency Name']
Userchunksize = 100000

conn = mysql.connector.connect(user='root', password='root',
                              host='127.0.0.1',
                              database='BigData')

cursor = conn.cursor()

conn.rollback()
print("X")
time.sleep(10)
#
#First, we should create database. Use BigData1.sql to create database and table.
#Then, we should prepare data file - you can skip this part, if you've got fitting dataset. We want only 3 columns (it saves a lot of time.)
#Result: new.csv -> 4 columns ( ID and all necesery 'Complaint Type', 'Borough', 'Agency Name'  )
#

#Data preparation
"""
start_time_processData = time.time()
df = pd.read_csv("data.csv", usecols=fields)
print("OUT")
df.to_csv("new.csv")
print("--- %s seconds ---" % (time.time() - start_time_processData))
#Data preparation END
"""

#Inserting data to database
#Option1 - faster

#datafile (new.csv) should be there: SHOW VARIABLES LIKE "secure_file_priv"; 
sqlQuery = "LOAD DATA  INFILE 'D:/Programy/MySQL/MySQL Server 8.0/Uploads/new.csv' INTO TABLE info FIELDS TERMINATED BY ','  ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (Id,AgencyName,ComplaintType, Borough);"

start_time_process111 = time.time()
cursor.execute(sqlQuery)
print("--- %s seconds ---" % (time.time() - start_time_process111))
#Without commit.  Commit is killing my computer

#Option1 - END

"""
#Inserting data to database
#Option2 - slower, using chunks

insertQuery = "insert into info (ComplaintType, AgencyName, Borough) values (%s, %s, %s)"
print("---------------Insert---------------------")

start_time_insert = time.time()
for chunk in pd.read_csv("data.csv", chunksize=Userchunksize, usecols=fields):
    for index, row in chunk.iterrows():
        val = (row[1], row[0], row[2])
        cursor.execute(insertQuery, val)
    conn.commit()
    print("---------------Commit---------------------")
cursor.close()
print("--- %s seconds ---" % (time.time() - start_time_insert))
print("---------------Insert-END---------------------")
print("")

#Option2 END
"""

#1
print("---------------P1---------------------")
start_time_process1 = time.time()
cursor.execute('select ComplaintType, count(*) as cnt from info group by ComplaintType order  by cnt desc limit 1')

print("--- %s seconds ---" % (time.time() - start_time_process1))
for row in cursor:
    print(row)
print("---------------------------------------")

#2 extremely slow ~11h
print("---------------P2---------------------")
start_time_process2 = time.time()
cursor.execute('select Borough, ComplaintType,  count(ComplaintType) as cnt from info as i group by Borough, ComplaintType having i.ComplaintType = (\
				select  i2.ComplaintType from info i2 where i2.Borough = i.Borough group by i2.ComplaintType order by count(*) desc, i2.ComplaintType limit 1)')

print("--- %s seconds ---" % (time.time() - start_time_process2))
for row in cursor:
    print(row)
print("---------------------------------------")

#3
print("---------------P3---------------------")
start_time_process3 = time.time()
cursor.execute('select AgencyName, count(*) as cnt from info group by AgencyName order by cnt desc limit 1')

print("--- %s seconds ---" % (time.time() - start_time_process3))
for row in cursor:
    print(row)
print("---------------------------------------")



