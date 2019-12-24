#把所有对数据库操作的逻辑封装在一段代码里
from DBUtils.PooledDB import PooledDB
import  pymysql
import configparser

class KeenDBUtill(object):
    parser = configparser.ConfigParser()
    parser.read("mysql.conf")

    mysql_host = parser['mysql']['host']
    mysql_port = int(parser['mysql']['port'])
    mysql_username = parser['mysql']['username']
    mysql_password = parser['mysql']['password']
    mysql_maxconnections = int(parser['mysql']['max_maxconnections'])
    mysql_databases = parser['mysql']['databases']

    mysqlPool = PooledDB(pymysql,
                         maxconnections=mysql_maxconnections,
                         host=mysql_host,
                         user=mysql_username,
                         passwd=mysql_password,
                         db=mysql_databases,
                         port=mysql_port)

    def __init__(self):
        pass

    def get_connection(self):
        return KeenDBUtill.mysqlPool.connection()

    @classmethod
    def get_connection_pool(cls):

        return cls.mysqlPool


