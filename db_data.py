'''
Author: guo_MateBookPro 867718012@qq.com
Date: 2022-02-25 22:15:27
LastEditors: guo_MateBookPro 867718012@qq.com
LastEditTime: 2023-08-12 15:55:09
FilePath: /pyiec104sqlite-main/db_data.py
Description: 人一生会遇到约2920万人,两个人相爱的概率是0.000049,所以你不爱我,我不怪你.
Copyright (c) 2023 by ${git_name} email: ${git_email}, All Rights Reserved.
'''
from db_mysql import db
import datetime
import time
import pandas as pd
from collections import defaultdict
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

# class db_data(db):
# 	"""docstring for db_data"""

# 	def __init__(self, filename=None):
# 		if not isinstance(filename,str):
# 			filename = get_todayfilename()
# 		super(db_data, self).__init__(filename,"data_create.sqlite")
# 		self.time= datetime.datetime.now().strftime("%Y-%m")
# 		# self.last_insert_time = 0

# 	def put(self,buf):
# 		if (len(buf)==0): 
# 			return
# 		# current_time = time.time()
# 		if datetime.datetime.now().strftime("%Y-%m") != self.time:
# 			self.time= datetime.datetime.now().strftime("%Y-%m")
# 			self.close()
# 			self.__init__()
# 		cursor = self.sql.cursor()
# 		#! 以下为为添加

# 		for (addr, dt, v, q) in buf:
# 			#dt_minute = dt[:-3]  # 格式化dt字段，只保留分钟部分
# 			dt_minute = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# 			if addr in data_dict:
# 				item_name = data_dict[addr][0]
# 				item_unit = data_dict[addr][1]
# 			else:
# 				item_name = "未知"
# 				item_unit = "未知"
# 			query = f"""INSERT INTO {self.table_name} (create_time, item_addr, item_name, item_unit, item_val) 
#    				VALUES ('{dt_minute}', '{addr}', '{item_name}', '{item_unit}', '{v}');"""
# 			# query = f"""INSERT INTO {self.table_name} (create_time, item_addr, item_name, item_unit, item_val)
# 			# 	SELECT '{dt_minute}', '{addr}', '{item_name}', '{item_unit}', '{v}'
# 			# 	WHERE NOT EXISTS (SELECT 1 FROM {self.table_name} WHERE create_time = '{dt_minute}' AND item_addr = '{addr}'
# 			# 	)"""
			
# 			try:
# 				cursor.execute(query)
# 				self.commit()
# 			except  Exception as e:
# 				print("")
# 				# 处理唯一约束冲突的逻辑

   



# 		# for (addr, dt, v, q) in buf:
# 		# 	query = f'INSERT INTO data (id, dt, v, q) VALUES ({addr}, "{dt}", {v}, {q});'

# 		# 	cursor.executescript(query)

# 		cursor.close()

class db_data(db):
    """docstring for db_data"""

    def __init__(self, filename=None):
        if not isinstance(filename, str):
            filename = get_todayfilename()
        super(db_data, self).__init__(filename, "data_create.sqlite")
        self.time = datetime.datetime.now().replace(second=0, microsecond=0)



    def put(self, buf):
        if len(buf) == 0:
            return
        current_time = datetime.datetime.now()
        current_time = current_time.replace(second=0, microsecond=0)
        
		# 数据字典，用于存储每个item_addr在五分钟间隔内的数据
        self.data_dict_value = defaultdict(lambda: {'values': [], 'timestamps': []})


        # if (current_time - self.time) >= datetime.timedelta(minutes=1):
            
        #     self.data_dict = defaultdict(lambda: {'values': [], 'timestamps': []})
        #     cursor = self.sql.cursor()

        #     for (addr, dt, v, q) in buf:
        #         self.data_dict_value[addr]['values'].append(v)
        #         self.data_dict_value[addr]['timestamps'].append(dt)
        #         # print(self.data_dict_value)

        #         for addr, data in self.data_dict_value.items():
        #             if data['timestamps']:
        #                 # 计算间隔内的平均值
        #                 avg_value = sum(data['values']) / len(data['values'])
        #                 print(avg_value)
        #                 # 从这个间隔的第一个数据点获取时间戳
        #                 dt_minute = data['timestamps'][0]
        #                 if addr in data_dict:
        #                     item_name = data_dict[addr][0]
        #                     item_unit = data_dict[addr][1]
        #                 else:
        #                     item_name = "未知"
        #                     item_unit = "未知"

        #                 # 将平均值插入到数据库中
        #                 query = f"""INSERT INTO {self.table_name} (create_time, item_addr, item_name, item_unit, item_val) 
        #                         VALUES ('{dt_minute}', '{addr}', '{item_name}', '{item_unit}', '{avg_value}');"""

        #                 try:
        #                     cursor.execute(query)
        #                 except Exception as e:
        #                     print(f"插入数据时出错: {e}")    
        #     self.commit()
        #     cursor.close() 
        #      # 重置数据字典并更新时间戳
        #     self.time = current_time   
        for (addr, dt, v, q) in buf:
            self.data_dict_value[addr]['values'].append(v)
            self.data_dict_value[addr]['timestamps'].append(dt)

        if (current_time - self.time) >= datetime.timedelta(minutes=1):
            cursor = self.sql.cursor()

            for addr, data in self.data_dict_value.items():
                if data['timestamps']:
                    # 计算间隔内的平均值
                    avg_value = sum(data['values']) / len(data['values'])
                    print(avg_value)
                    # 从这个间隔的第一个数据点获取时间戳
                    dt_minute = data['timestamps'][0]
                    if addr in data_dict:
                        item_name = data_dict[addr][0]
                        item_unit = data_dict[addr][1]
                    else:
                        item_name = "未知"
                        item_unit = "未知"

                    # 将平均值插入到数据库中
                    query = f"""INSERT INTO {self.table_name} (create_time, item_addr, item_name, item_unit, item_val) 
                            VALUES ('{dt_minute}', '{addr}', '{item_name}', '{item_unit}', '{avg_value}');"""

                    try:
                        cursor.execute(query)
                    except Exception as e:
                        print(f"插入数据时出错: {e}")

            self.commit()
            cursor.close()

            # 重置数据字典并更新时间戳
            self.data_dict_value = defaultdict(lambda: {'values': [], 'timestamps': []})
            self.time = current_time
        

                    
                        
                        
                    

