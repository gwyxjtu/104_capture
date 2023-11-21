from datetime import datetime
import psycopg2

class db(object):
    """docstring for db"""
    closed = True
    # sql = None
    filename = None

    def __init__(self, filename, createscript=None):
        self.filename = filename
        # self.sql=psycopg2.connect(host='192.168.3.13', port=5432, user='ems', password='Yulin@0903', database='ems_capture')
        self.sql = psycopg2.connect(host='123.249.70.226', port=7004, user='postgres', password='postgres', database='ems_capture')
        self.create_table()
        self.open()

    def create_table(self):
        time=datetime.now().strftime("%Y_%m")
        t_n="test20_104_5min_"+time
        self.table_name=t_n
        self.createtable="""CREATE TABLE IF NOT EXISTS public."""+t_n+"""
        (
            gid SERIAL PRIMARY KEY,
            create_time timestamp without time zone NOT NULL,
            item_addr character varying(20) COLLATE pg_catalog."default" NOT NULL,
            item_name character varying(100) COLLATE pg_catalog."default",
            item_unit character varying(200) COLLATE pg_catalog."default",
            item_val character varying(100) COLLATE pg_catalog."default"
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS public."""+t_n+"""
            OWNER to postgres;

        COMMENT ON TABLE public."""+t_n+"""
            IS '存储许继数据，一分钟存储一次。';

        COMMENT ON COLUMN public."""+t_n+""".gid
            IS '检索主键';

        COMMENT ON COLUMN public."""+t_n+""".create_time
            IS '数据插入的时间';

        COMMENT ON COLUMN public."""+t_n+""".item_addr
            IS '点位数据地址';

        COMMENT ON COLUMN public."""+t_n+""".item_name
            IS '点位数据名称或备注';

        COMMENT ON COLUMN public."""+t_n+""".item_unit
            IS '点位数据单位或范围';

        COMMENT ON COLUMN public."""+t_n+""".item_val
            IS '数据值';"""
        # +"""ALTER TABLE """+t_n+" ADD CONSTRAINT unique_create_time_item_addr UNIQUE (create_time, item_addr);"

        
        
        
        # if (filename and isinstance(filename,str)):
        #     self.open()

        
        cursor = self.sql.cursor()
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{t_n}')")
        
        
        result = cursor.fetchone()[0]
        

        if not result:
            # 如果表不存在，则创建新表
            cursor = self.sql.cursor()
            cursor.execute(self.createtable)
            self.sql.commit()
            self.sql.close()
            self.closed = True

        



    def __del__(self):
        self.sql.close()
    def open (self):
        # self.sql=psycopg2.connect(host='192.168.3.13', port=5432, user='ems', password='Yulin@0903', database='ems_capture')
        self.sql = psycopg2.connect(host='123.249.70.226', port=7004, user='postgres', password='postgres', database='ems_capture')
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
 
            cursor.execute(self.createtable)
            cursor.close()
            return True
        except Exception as e:
            return False

    def query(self, query):
        if self.closed:
            return False
        try:
            cursor = self.sql.cursor()
            cursor.execute(query)
            self.sql.commit()
            cursor.close()
            return True
        except Exception as e:
            return False
