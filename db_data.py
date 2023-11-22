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
import threading
import pandas as pd
from collections import defaultdict
from dateutil.relativedelta import relativedelta
import calendar
df = pd.read_excel("104_excel.xls")

df = df.drop(0)
data_dict = {}
for idx, row in df.iterrows():
    info_address = row.iloc[0]
    #转为10进制
    info_address_10 = int(info_address,16)
    info_name = row.iloc[1]
    data_range = row.iloc[2]
    data_dict[info_address_10] = [info_name,data_range]


def get_todayfilename():
	return time.strftime("%Y%m.sqlite")

# ###################################
# 跨越整数小时出现问题
# 115	2023-11-21 11:35:00	16385	未知	未知	3.200000047683716
# 1304	2023-11-21 11:40:00	16385	未知	未知	3.108474711240348
# 2550	2023-11-21 11:45:00	16385	未知	未知	3.1211539690311136
# 3289	2023-11-21 11:50:00	16385	未知	未知	3.1196970903512202
# 4903	2023-11-21 11:55:00	16385	未知	未知	3.119607967488906
# 5667	2023-11-21 11:00:00	16385	未知	未知	3.1086957869322402
# 6856	2023-11-21 12:05:00	16385	未知	未知	3.1113208869718156
# class db_data(db):

#     def __init__(self, filename=None):
#         if not isinstance(filename, str):
#             filename = get_todayfilename()
#         super(db_data, self).__init__(filename, "data_create.sqlite")

#         # 设置 self.time 为当前时间的整五分钟
#         current_time = datetime.datetime.now()
#         self.time = current_time - datetime.timedelta(minutes=current_time.minute % 5,
#                                                         seconds=current_time.second,
#                                                         microseconds=current_time.microsecond)
        
#         self.previous_upload_time = time.time()  # 记录上一次的上传时间

#         # 数据字典，用于存储每个item_addr在五分钟间隔内的数据
#         self.data_dict_value = defaultdict(lambda: {'values': [], 'timestamps': []})

#     def put(self, buf):
#         if len(buf) == 0:
#             return

#         current_time = datetime.datetime.now()
#         current_time = current_time.replace(second=0, microsecond=0)

#         # 遍历缓冲区，将数据存储到数据字典中
#         for (addr, dt, v, q) in buf:
#             self.data_dict_value[addr]['values'].append(v)
#             self.data_dict_value[addr]['timestamps'].append(dt)

#         # 计算数据处理的时间
#         processing_time = time.time() - self.previous_upload_time

#         # 等待到达下一个五分钟整数倍的时间点
#         next_upload_time = self.time + datetime.timedelta(minutes=5)
#         sleep_time = max(0, (next_upload_time - current_time).total_seconds())
#         time.sleep(sleep_time)

#         cursor = self.sql.cursor()

#         # 遍历数据字典，计算每个item_addr的平均值并插入数据库
#         for addr, data in self.data_dict_value.items():
#             if data['timestamps']:
#                 # 计算间隔内的平均值
#                 avg_value = sum(data['values']) / len(data['values'])
#                 # print(f"{addr}的平均值：{avg_value}")

#                 # 从这个间隔的第一个数据点获取时间戳
#                 dt_minute = data['timestamps'][0]
#                 print(f"{dt_minute},{addr}的平均值：{avg_value}")

#                 # 将字符串转换为 datetime 对象
#                 dt_minute = datetime.datetime.strptime(dt_minute, '%Y-%m-%d %H:%M:%S')

#                 # 调整时间戳为下一个五分钟整数倍的格式
#                 dt_minute = dt_minute.replace(second=0, microsecond=0, minute=(dt_minute.minute // 5 + 1) * 5 % 60)

#                 # 检查是否已经插入了相同的 dt_minute，如果是，则跳过插入
#                 query_check = f"SELECT COUNT(*) FROM {self.table_name} WHERE item_addr = '{addr}' AND create_time = '{dt_minute}';"
#                 cursor.execute(query_check)
#                 count = cursor.fetchone()[0]

#                 if count == 0:
#                     if addr in data_dict:
#                         item_name = data_dict[addr][0]
#                         item_unit = data_dict[addr][1]
#                     else:
#                         item_name = "未知"
#                         item_unit = "未知"

#                     # 将平均值插入到数据库中
#                     query = f"""INSERT INTO {self.table_name} (create_time, item_addr, item_name, item_unit, item_val) 
#                             VALUES ('{dt_minute}', '{addr}', '{item_name}', '{item_unit}', '{avg_value}');"""

#                     try:
#                         cursor.execute(query)
#                     except Exception as e:
#                         print(f"插入数据时出错: {e}")

#         self.commit()
#         cursor.close()

#         # 重置数据字典并更新时间戳
#         self.data_dict_value = defaultdict(lambda: {'values': [], 'timestamps': []})
#         self.time = current_time

#         # 更新上一次的上传时间
#         self.previous_upload_time = time.time()


class db_data(db):

    def __init__(self, filename=None):
        if not isinstance(filename, str):
            filename = get_todayfilename()
        super(db_data, self).__init__(filename, "data_create.sqlite")

        # 设置 self.time 为当前时间的整五分钟
        current_time = datetime.datetime.now()
        self.time = current_time - datetime.timedelta(minutes=current_time.minute % 5,
                                                        seconds=current_time.second,
                                                        microseconds=current_time.microsecond)
        
        self.previous_upload_time = time.time()  # 记录上一次的上传时间

        # 数据字典，用于存储每个item_addr在五分钟间隔内的数据
        self.data_dict_value = defaultdict(lambda: {'values': [], 'timestamps': []})

    def put(self, buf):
        if len(buf) == 0:
            return

        current_time = datetime.datetime.now()
        current_time = current_time.replace(second=0, microsecond=0)

        # 遍历缓冲区，将数据存储到数据字典中
        for (addr, dt, v, q) in buf:
            self.data_dict_value[addr]['values'].append(v)
            self.data_dict_value[addr]['timestamps'].append(dt)

        # 计算数据处理的时间
        processing_time = time.time() - self.previous_upload_time

        # 等待到达下一个五分钟整数倍的时间点
        next_upload_time = self.time + datetime.timedelta(minutes=5)
        sleep_time = (next_upload_time - current_time).total_seconds()
        # sleep_time = max(0, (next_upload_time - current_time).total_seconds())
        if sleep_time > 0:
             time.sleep(sleep_time)

        cursor = self.sql.cursor()

        # 遍历数据字典，计算每个item_addr的平均值并插入数据库
        for addr, data in self.data_dict_value.items():
            if data['timestamps']:
                # 计算间隔内的平均值
                avg_value = sum(data['values']) / len(data['values'])
                # print(f"{addr}的平均值：{avg_value}")

                # 从这个间隔的第一个数据点获取时间戳
                dt_minute = data['timestamps'][0]
                print(f"{dt_minute},{addr}的平均值：{avg_value}")

                # 将字符串转换为 datetime 对象
                dt_minute = datetime.datetime.strptime(dt_minute, '%Y-%m-%d %H:%M:%S')

                # 调整时间戳为下一个五分钟整数倍的格式
                dt_minute = dt_minute + datetime.timedelta(minutes=5 - dt_minute.minute % 5)

                # 处理小时、天、月和年变化
                if dt_minute.minute == 0 and dt_minute.second == 0:
                    # 如果分钟为0，秒为0，说明已经跨到下一个小时，将小时加1
                    dt_minute = dt_minute.replace(hour=(dt_minute.hour + 1) % 24)

                    # 处理天、月、年变化
                    if dt_minute.hour == 0:
                        # 如果小时为0，说明已经跨到下一天，将天数加1
                        dt_minute = dt_minute.replace(day=(dt_minute.day + 1))
                        
                        # 处理月变化
                        if dt_minute.day == 1:
                            # 如果天数为1，说明已经跨到下一个月，将月份加1
                            dt_minute = dt_minute.replace(month=(dt_minute.month + 1) % 13)
                            
                            # 处理年变化
                            if dt_minute.month == 1:
                                # 如果月份为1，说明已经跨到下一年，将年份加1
                                dt_minute = dt_minute.replace(year=(dt_minute.year + 1))
                                
                dt_minute = dt_minute.replace(second=0, microsecond=0)
                # 检查是否已经插入了相同的 dt_minute，如果是，则跳过插入
                query_check = f"SELECT COUNT(*) FROM {self.table_name} WHERE item_addr = '{addr}' AND create_time = '{dt_minute.strftime('%Y-%m-%d %H:%M:%S')}';"
                cursor.execute(query_check)
                count = cursor.fetchone()[0]

                if count == 0:
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

        # 更新上一次的上传时间
        self.previous_upload_time = time.time()
