'''
Author: guo_MateBookPro 867718012@qq.com
Date: 2022-02-25 22:15:27
LastEditors: guo_MateBookPro 867718012@qq.com
LastEditTime: 2023-08-12 16:26:56
FilePath: /pyiec104sqlite-main/main.py
Description: 人一生会遇到约2920万人,两个人相爱的概率是0.000049,所以你不爱我,我不怪你.
Copyright (c) 2023 by ${git_name} email: ${git_email}, All Rights Reserved.
'''
import logging
import time
from multiprocessing import Process, Queue

from iec104.client import iec104_client
from db_data import db_data

host,port = '192.168.3.21', 2404

QUEUE_RECIEVER_SLEEP = 2 	# seconds
COMMIT_COUNTER = 30 		# 30xQUEUE_RECIEVER_SLEEP seconds
def log_init():
	log = logging.getLogger('test')
	log.setLevel(logging.INFO)
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	#file log
	tofile = logging.FileHandler('test.log','a')
	tofile.setLevel(logging.INFO)
	tofile.setFormatter(formatter)
	#console log
	toconsole = logging.StreamHandler()
	toconsole.setLevel(logging.INFO)
	toconsole.setFormatter(formatter)
	log.addHandler(tofile)
	log.addHandler(toconsole)
	log.info('')
	log.info("New log started")
	return log

log = log_init()

###############################################################################

def queue_data_creator(q):
	client = iec104_client(host,port)
	client.on_i_frame = q.put
	client.start()

###############################################################################

def queue_data_reciever(q):
	db = db_data()
	counter = 0
	counter2 = 0
	day = time.localtime().__getitem__(4)
	while True:
		if not (day == time.localtime().__getitem__(4)):
			db.close()
			log.info("Create new sqlite file")
			db = db_data()
			day = time.localtime().__getitem__(4)
		counter = 0
		buf36 = []
		while not q.empty():
			counter+=1
			asdu = q.get()
			# log.info([ioa.TypeId for ioa in asdu.ioa])
			for ioa in asdu.ioa:
				if ioa.TypeId != 13:
					continue
				buf36.append((ioa.addr,str(ioa.time), ioa.value, ioa.quality))
		
		counter2 += 1
		db.put(buf36)
		if (counter2>=COMMIT_COUNTER):
			counter2 = 0
			db.commit()
		time.sleep(QUEUE_RECIEVER_SLEEP)

###############################################################################

if __name__ == '__main__':
	q = Queue()
	creator = Process(target=queue_data_creator, args=(q,))
	reciever = Process(target=queue_data_reciever, args=(q,))
	
	try:
		creator.start()
		reciever.start()
	except Exception as e:
		raise e
	finally:
		q.close()
		q.join_thread()
		creator.join()
		reciever.join()
