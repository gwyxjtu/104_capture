# **IEC104采集代码**
- 主要内容： 基于IEC104协议采集榆林零碳能源站项目的许继集控柜中的遥测数据


## **0. 代码功能解释**
- 本代码为实时采集榆林能源站遥测数据
- db_mysql.py创建数据库
- db_data.py间隔五分钟遍历缓冲区获取各个点位五分钟内的平均值

## **1. 代码环境**
 
```
psycopg2>=2.9.9
pandas>=2.1.1
```

## **2. 代码运行说明**
- 采集遥测数据   
```
python main.py
```   
- 更改host, port
```
榆林能源站服务器使用：'192.168.3.21', 2404
自己电脑使用：'123.249.70.226', 7003
需要同时更改main.py和db_mysql中的host、port
```



## **3.输出**：
- 输出：遥测相关点位的数据
- 以17573点位数据说明输出结果

|  gid  |create\_time| item\_addr | item\_name| item\_unit| item\_val |
| ----- |------------|------------|-----------|-----------|-----------|
| 3289206  | 2023-12-12 14:55:00 | 17573 | 未知 | 未知 | 0.0  | 
| 3290395  | 2023-12-12 15:00:00 | 17573 | 未知 | 未知 | 0.0  | 
| 3291590  | 2023-12-12 15:05:00 | 17573 | 未知 | 未知 | 0.0  | 
| 3292779  | 2023-12-12 15:10:00 | 17573 | 未知 | 未知 | 0.0  | 
| 3293968  | 2023-12-12 15:15:00 | 17573 | 未知 | 未知 | 0.0  | 

## **4. 代码目录**

```
104_capture:.
│  .gitignore
│  104_excel.xls
│  104_excelv1.xls
│  data_create.sqlite
│  db.py
│  db_connect.py
│  db_data.py
│  db_mysql.py
│  excel_text.py
│  heat_db.py
│  IEC104client.log
│  LICENSE
│  main.py
│  mytree.txt
│  README.md
│  test.log
│  test104.py
│  value.py
│  
├─.vscode
│      settings.json
│      
├─iec104
│  │  asdu.py
│  │  client.py
│  │  define.py
│  │  ioa.py
│  │  
│  └─__pycache__
│          asdu.cpython-310.pyc
│          asdu.cpython-311.pyc
│          client.cpython-310.pyc
│          client.cpython-311.pyc
│          define.cpython-310.pyc
│          define.cpython-311.pyc
│          ioa.cpython-310.pyc
│          ioa.cpython-311.pyc
│          
└─__pycache__
        db.cpython-310.pyc
        db.cpython-311.pyc
        db_data.cpython-310.pyc
        db_data.cpython-311.pyc
        db_mysql.cpython-310.pyc
        db_mysql.cpython-311.pyc
```


## **5. 存在的问题**
- 间隔三小时会丢失1-2次数据

