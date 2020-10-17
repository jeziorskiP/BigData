from sklearn.model_selection import train_test_split
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import pyodbc 
import time

from sklearn.datasets import load_iris

fields = ['Complaint Type', 'Borough', 'Agency Name']
Userchunksize = 100000
"""

LoadOption1 -> wczytwyanie po Userchunksize 
LoadOption2 -> wczytwyanie całości
"""

#LoadOption1
print("---------------Load---------------------")
start_time_insert = time.time()
chunk_list = []
for chunk in pd.read_csv("data.csv", chunksize=Userchunksize, usecols=fields):
    chunk_list.append(chunk)
df_concat = pd.concat(chunk_list)
print("--- %s seconds ---" % (time.time() - start_time_insert))
print("---------------Load-END-----------------")
print("")
df = df_concat



#LoadOption2
print("---------------Load---------------------")
start_time_insert2 = time.time()
df = pd.read_csv("data.csv", usecols=fields )
print("--- %s seconds ---" % (time.time() - start_time_insert2))
print("---------------Load-END-----------------")
print("")



#1
print("---------P1.1--Complaint Type---------------------")
start_time_process1 = time.time()
onlyComplaimentType = df['Complaint Type']
onlyComplaimentType.value_counts().index[0]

print("--- %s seconds ---" % (time.time() - start_time_process1))
print(onlyComplaimentType.value_counts().index[0])
print("---------------!Complaint Type!-----------------\n")


print("---------P1.2--Complaint Type---------------------")
start_time_process11 = time.time()
onlyComplaimentType = df.groupby(['Complaint Type']).size().idxmax(0)
print("--- %s seconds ---" % (time.time() - start_time_process11))
print(onlyComplaimentType)
print("---------------!Complaint Type!-----------------\n\n")

#2
print("---------P2.1--Agency Name-------------------------")
start_time_process2 = time.time()
onlyAgencyName = df['Agency Name']
onlyAgencyName.value_counts().index[0]

print("--- %s seconds ---" % (time.time() - start_time_process2))
print(onlyAgencyName.value_counts().index[0])
print("---------------!Agency Name!---------------------\n")

print("---------P2.2--Agency Name------------------ -------")
start_time_process22 = time.time()
onlyAgencyName = df.groupby([ 'Agency Name' ]).size().idxmax(0)

print("--- %s seconds ---" % (time.time() - start_time_process22))
print(onlyAgencyName)
print("---------------!Agency Name!---------------------\n\n")

#3
print("--------P3.1--Complaint Type per Borough-----------")
start_time_process3 = time.time()

boroughs = df['Borough'].unique()

for borough in boroughs:
    last = df[ df['Borough']== borough ].groupby('Borough')['Complaint Type'].value_counts().index[0]
    #print(last)
print("--- %s seconds ---" % (time.time() - start_time_process3))
print("----------!Complaint Type per Borough!------------------\n")

#3
print("--------P3.2--Complaint Type per Borough-----------")
start_time_process31 = time.time()
res = df.groupby('Borough')['Complaint Type']\
    .apply(pd.Series.value_counts)\
    .unstack().idxmax(axis=1)
print("--- %s seconds ---" % (time.time() - start_time_process31))
print(res)
print("----------!Complaint Type per Borough!------------------\n")

