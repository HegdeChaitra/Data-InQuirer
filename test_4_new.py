from test_3 import *
#!user/bin/env python
import sys
import numpy as np
import os
import pandas as pd
from csv import reader
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
#x = spark.read.json('/user/bigdata/nyc_open_data/yuzm-c784.json',multiLine=True)
file_names  = [line.rstrip('\n') for line in open('file_names_2.txt')]
big_files = [line.rstrip('\n') for line in open('top_20_big_files.txt')]
info_list = ['meta.view.attributionLink','meta.view.category','meta.view.attribution', 'meta.view.columns', 'meta.view.description','meta.view.id','meta.view.downloadCount','meta.view.name','meta.view.viewCount','meta.view.tags']
list_f = []
list_atl = []
list_cat = []
list_at = []
list_col = []
list_desc = []
list_id = []
list_downc = []
list_name = []
list_viewc = []
list_tags = []
list_all = [list_atl,list_cat,list_at,list_col,list_desc, list_id, list_downc, list_name,list_viewc, list_tags]
for k,f in enumerate(file_names):
	if f in big_files:
		continue
	df_temp = spark.read.json(f,multiLine = True)
	print(k)
	for i,j in enumerate(info_list):
		try:
			val = (df_temp.select(j).collect())[0][0]
		except:
			val = None
		list_all[i].append(val)
	list_f.append(f)
list_all.append(list_f)
columns = ['attributionLink','category','attribution','columns','description','id','downloadCount','name','viewCount','tags','file_path']
df = pd.DataFrame(np.transpose(list_all),columns = columns)
df.to_csv('meta_data.csv', index = False)
#	df_temp.printSchema()
#	try:
#		atl = (df_temp.select('meta.view.attributionLink').collect())[0][0]
#	except:
#		atl = None
#	list_atl.append(atl)
#	try:
#		cat = (df_temp.select('meta.view.category').collect())[0][0]
#	except:
#		cat = None
#	list_cat.append(cat)	
#x_pd = x.select("meta").toPandas()
#x_dict = x_pd.to_dict()
#print(x_dict.keys())
#y = x.select('meta.view.attributionLink').collect()
#print(y)
