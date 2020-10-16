from sklearn.model_selection import train_test_split
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import pyodbc 
import time 


from sklearn.datasets import load_iris

fields = ['Complaint Type', 'Borough', 'Agency Name']
Userchunksize = 100000

conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=(localdb)\MSSQLLocalDB;'
                      'Database=BigData;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

insertQuery = '''insert into info (ComplaintType, AgencyName, Borough) values (?, ?, ?);'''
print("---------------Insert---------------------")

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


