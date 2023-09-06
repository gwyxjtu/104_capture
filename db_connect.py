import mysql.connector
import pymysql
import psycopg2
class MySQLConnection:
    def __init__(self, host, username, password, database):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.connection = None
    
    def connect(self):
        print("正在连接到MySQL数据库...")
        try:
            self.connection = psycopg2.connect(host='123.249.70.226', port=7004, user='postgres', password='postgres', database='iot_obix_control')

            
            print("成功连接到MySQL数据库")
        except mysql.connector.Error as error:
            print("连接到MySQL数据库时发生错误: {}".format(error))
    
    def execute_query(self, query):
        if not self.connection:
            print("请先连接到MySQL数据库")
            return
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            for row in result:
                print(row)
        except mysql.connector.Error as error:
            print("执行SQL语句时发生错误: {}".format(error))
    
    def close(self):
        if self.connection:
            self.connection.close()
            print("成功关闭MySQL连接")

# 示例用法
# 创建一个MySQLConnection实例
mysql_conn = MySQLConnection("localhost", "username", "password", "mydatabase")

# 连接到数据库
mysql_conn.connect()

# 执行SQL语句
mysql_conn.execute_query("SELECT * FROM mytable")

# 关闭连接
mysql_conn.close()
