from sklearn.model_selection import train_test_split
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import pyodbc 
import time

from sklearn.datasets import load_iris

fields = ['Complaint Type', 'Borough', 'Agency Name']
Userchunksize = 100000

#df = pd.read_csv("data.csv", usecols=['Agency Name', 'Complaint Type', 'Borough'])

print("---------------Load---------------------")
start_time_insert = time.time()
for chunk in pd.read_csv("data.csv", chunksize=Userchunksize, usecols=fields):

print("--- %s seconds ---" % (time.time() - start_time_insert))
print("---------------Load-END-----------------")
print("")


#1
print("---------P1--Complaint Type---------------------")
start_time_process1 = time.time()
onlyComplaimentType = chunk['Complaint Type']
onlyComplaimentType.value_counts()

print("--- %s seconds ---" % (time.time() - start_time_process1))
print(onlyComplaimentType.value_counts())
print("---------------!Complaint Type!-----------------")

#2
print("---------P2--Agency Name-------------------------")
start_time_process2 = time.time()
onlyAgencyName = chunk['Agency Name']
onlyAgencyName.value_counts()

print("--- %s seconds ---" % (time.time() - start_time_process2))
print(onlyAgencyName.value_counts())
print("---------------!Agency Name!---------------------")

#3
print("--------P3--Complaint Type per Borough-----------")
start_time_process3 = time.time()
last = chunk.groupby('Borough')['Complaint Type'].value_counts()
print("--- %s seconds ---" % (time.time() - start_time_process3))
print(last)
print("----------!Complaint Type per Borough!------------------")