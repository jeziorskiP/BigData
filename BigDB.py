from sklearn.model_selection import train_test_split
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import pyodbc 
import time 


from sklearn.datasets import load_iris

fields = ['Complaint Type', 'Borough', 'Agency Name']
Userchunksize = 100000
#
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=(localdb)\MSSQLLocalDB;'
                      'Database=BigData;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()


"""
Option 1-> slow loading, using  chunks. 
Option 2-> faster, but you have to prepare file with appropriate columns (fields)
You should comment/uncommnet one option.
"""
print("---------------Insert---------------------")



"""
#option1 - slow, using chunks

insertQuery = '''insert into info (ComplaintType, AgencyName, Borough) values (?, ?, ?);'''
start_time_insert = time.time()
for chunk in pd.read_csv("data.csv", chunksize=Userchunksize, usecols=fields):
    for index, row in chunk.iterrows():
        cursor.execute(insertQuery, row[1], row[0], row[2])
    conn.commit()    
    print("---------------Commit---------------------")

cursor.close()
print("--- %s seconds ---" % (time.time() - start_time_insert))
print("---------------Insert-END---------------------")
print("")
"""


#Option2 faster, but with special sprepared file
#Slice unused column(1. Load data 2. Slice data 3. Write data)
#
#First, we should create database. Use SqlQuery.sql to create database and table.
#Then, we should prepare data file - you can skip this part, if you've got fitting dataset. We want only 3 columns (it saves a lot of time.)
#Result: new.csv -> 4 columns ( ID and all necesery 'Complaint Type', 'Borough', 'Agency Name'  )
#

start_time_load = time.time()
df = pd.read_csv("data.csv", usecols=fields)
print("--- %s seconds ---" % (time.time() - start_time_load))
print("OUT")
start_time_write= time.time()
df.to_csv("new.csv")

print("--- %s seconds ---" % (time.time() - start_time_write))

print("Processing time: %s secunds" % (time.time() - start_time_load))


sql = """
BULK INSERT BigData..info
FROM 'D:\\Programy\\BigData\\new.csv' WITH (
    FIELDTERMINATOR=',',
    ROWTERMINATOR='\\n',
    firstrow = 2
    )
"""
   
start_time_insert2 = time.time() 
cursor.execute(sql)
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#Commit is killing my computer!
conn.commit()
print("--- %s seconds ---" % (time.time() - start_time_insert2))
print("---------------Insert-END---------------------")
print("")
cursor.close()

print("---------------inserted---------------------")




#1
print("---------------P1---------------------")
start_time_process1 = time.time()
cursor.execute('select top(1) ComplaintType, count(*) as cnt from info group by ComplaintType order  by cnt desc')

print("--- %s seconds ---" % (time.time() - start_time_process1))
for row in cursor:
    print(row)
print("---------------------------------------")

#2
print("---------------P2---------------------")
start_time_process2 = time.time()
cursor.execute('select Borough, ComplaintType,  count(ComplaintType) as cnt from info as i group by Borough, ComplaintType having i.ComplaintType = (\
				select top 1 i2.ComplaintType from info i2 where i2.Borough = i.Borough group by i2.ComplaintType order by count(*) desc, i2.ComplaintType)')

print("--- %s seconds ---" % (time.time() - start_time_process2))
for row in cursor:
    print(row)
print("---------------------------------------")
             
#3
print("---------------P3---------------------")
start_time_process3 = time.time()
cursor.execute('select top(1) AgencyName, count(*) as cnt from info group by AgencyName order by cnt desc')

print("--- %s seconds ---" % (time.time() - start_time_process3))
for row in cursor:
    print(row)
print("---------------------------------------")


