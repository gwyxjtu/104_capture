'''
Author: guo_MateBookPro 867718012@qq.com
Date: 2022-02-25 22:15:27
LastEditors: guo_MateBookPro 867718012@qq.com
LastEditTime: 2023-08-12 15:55:09
FilePath: /pyiec104sqlite-main/db_data.py
Description: 人一生会遇到约2920万人,两个人相爱的概率是0.000049,所以你不爱我,我不怪你.
Copyright (c) 2023 by ${git_name} email: ${git_email}, All Rights Reserved.
'''
import sqlite3
from db_mysql import db
import datetime
import time
import pandas as pd
df = pd.read_excel("104_excel.xls")

df = df.drop(0)
data_dict = {}
for idx, row in df.iterrows():
    info_address = row[0]
    #转为10进制
    info_address_10 = int(info_address,16)
    info_name = row[1]
    data_range = row[2]
    data_dict[info_address_10] = [info_name,data_range]


def get_todayfilename():
	return time.strftime("%Y%m.sqlite")

class db_data(db):
	"""docstring for db_data"""

	def __init__(self, filename=None):
		if not isinstance(filename,str):
			filename = get_todayfilename()
		super(db_data, self).__init__(filename,"data_create.sqlite")
		self.time= datetime.datetime.now().strftime("%Y-%m")

	def put(self,buf):
		if (len(buf)==0): 
			return
		if datetime.datetime.now().strftime("%Y-%m") != self.time:
			self.time= datetime.datetime.now().strftime("%Y-%m")
			self.close()
			self.__init__()
		cursor = self.sql.cursor()
		#! 以下为为添加

		for (addr, dt, v, q) in buf:
			#dt_minute = dt[:-3]  # 格式化dt字段，只保留分钟部分
			dt_minute = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			if addr in data_dict:
				item_name = data_dict[addr][0]
				item_unit = data_dict[addr][1]
			else:
				item_name = "未知"
				item_unit = "未知"
			query = f"""INSERT INTO {self.table_name} (create_time, item_addr, item_name, item_unit, item_val) VALUES ('{dt_minute}', '{addr}', '{item_name}', '{item_unit}', '{v}');"""
			# query = f'REPLACE INTO {self.table_name} VALUES ({addr}, "{dt_minute}", {v}, {q});'
			
			try:
				cursor.execute(query)
				self.commit()
			except sqlite3.IntegrityError:
				print("唯一约束冲突")
				# 处理唯一约束冲突的逻辑

   



		# for (addr, dt, v, q) in buf:
		# 	query = f'INSERT INTO data (id, dt, v, q) VALUES ({addr}, "{dt}", {v}, {q});'

		# 	cursor.executescript(query)

		cursor.close()

