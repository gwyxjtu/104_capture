'''
Author: guo_MateBookPro 867718012@qq.com
Date: 2023-08-09 18:52:06
LastEditors: guo_MateBookPro 867718012@qq.com
LastEditTime: 2023-08-12 15:06:46
FilePath: /pyiec104sqlite-main/test104.py
Description: 人一生会遇到约2920万人,两个人相爱的概率是0.000049,所以你不爱我,我不怪你.
Copyright (c) 2023 by ${git_name} email: ${git_email}, All Rights Reserved.
'''
from iec104.client import iec104_client

host,port = '192.168.3.21', 2404
host,port = '123.249.70.226', 7003
client = iec104_client(host,port)
client.test_start()