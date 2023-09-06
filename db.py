'''
Author: guo_MateBookPro 867718012@qq.com
Date: 2022-02-25 22:15:27
LastEditors: guo_MateBookPro 867718012@qq.com
LastEditTime: 2023-08-12 15:53:28
FilePath: /pyiec104sqlite-main/db.py
Description: 人一生会遇到约2920万人,两个人相爱的概率是0.000049,所以你不爱我,我不怪你.
Copyright (c) 2023 by ${git_name} email: ${git_email}, All Rights Reserved.
'''
import sqlite3

# //SELECT STRFTIME("%Y",when_ts)  FROM event

class db(object):
	"""docstring for db"""
	closed = True
	sql = None
	filename = None

	def __init__(self, filename, createscript=None):
		self.filename = filename
		if (filename and isinstance(filename,str)):
			self.open()
		if (createscript and isinstance(createscript,str)):
			self.executescript(createscript)

	def __del__(self):
		self.sql.close()

	def open (self):
		self.sql = sqlite3.connect(self.filename)
		self.closed = False

	def close(self):
		self.sql.close()
		self.closed = True
	def commit(self):
		self.sql.commit()
	def executescript(self, script):
		if self.closed:
			return False
		try:
			cursor = self.sql.cursor()
			with open(script, 'r') as sqlite_file:
				sql_script = sqlite_file.read()
			cursor.executescript(sql_script)
			cursor.close()
			return True
		except Exception as e:
			return False

	def query(self, query):
		if self.closed:
			return False
		try:
			self.sql.executescript(query)
			self.sql.commit()
			return True
		except Exception as e:
			return False
