#根据所要爬取的日期和该日期下有多少页记录，创建任务列表

import logging
import redis

from newSpider.KeenDBUtill import KeenDBUtill
from newSpider.KeenRedisUtill import KeenRedisUtill


class makeList(object):

    #任务列表表前缀
    TABLE_NAME_PREFIX="tb_list_{}"
    REDIS_PREFIX="list_{}"

    def __init__(self,start,end,totalPage,cnt=50):
        """
        @param start: 专利开始的时间点
        @param end:   专利结束的时间点
        @param totalPage: 在start-end这个区间内共有多少个专利
        @param cnt: 每页多少个专利，默认50个
        """

        self.start = start
        self.end = end
        self.totalPage = totalPage
        self.cnt = cnt
        #mysql表名称
        self.mysql_table_name = makeList.TABLE_NAME_PREFIX.format(self.start)
        #Redis列表名称
        self.redis_list_name = makeList.REDIS_PREFIX.format(self.start)


        #MYSQL Redis 连接池
        self.mysql_connection_pool =  KeenDBUtill.get_connection_pool()
        self.redis_connection_pool = KeenRedisUtill.get_connection_pool()

        #日志
        logging.basicConfig(level=logging.INFO, filename="makeList.log",filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def generate_mysql_list(self):
        """
        创建Mysql任务列表
        比如2018-01-01这一天更新了50页数据，那么就创建一个名为tb_list_2018-01-01的表，在这个表里插入50条记录，每条记录代表一页的信息存储在content字段里。
        与此同时，还要再Redis队列里创建50个记录，下一个爬虫从Redis队列里取出数据，爬取，更新content字段。
        @return:
        """


        SQL_CREATE_TABLE_IF_NOT_EXISTS = """
            CREATE TABLE  If Not Exists `{}`  (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `start` date NOT NULL,
          `end` date NOT NULL,
          `page` int(11) NOT NULL,
          `content` longblob NOT NULL,
          `flag` bit(1) NOT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """.format(self.mysql_table_name)

        SQL_INSERT_TASK_LIST = """ 
            insert into `{}` 
            (id,start,end,page,content,flag)
            values 
            (NULL,%s,%s,%s,'F',0)
        """.format(self.mysql_table_name)

        connection=None
        cursor = None
        try:
            self.logger.info("[MySql]--创建任务表[{}]".format(self.mysql_table_name))
            connection = self.mysql_connection_pool.connection()
            cursor = connection.cursor()
            cursor.execute(SQL_CREATE_TABLE_IF_NOT_EXISTS)
            self.logger.info("[MySql]--创建任务表[{}]完毕".format(self.mysql_table_name))

            self.logger.info("[MySql]--插入任务数据，共[{}]条".format(self.totalPage))
            for i in range(self.totalPage):
                cursor.execute(SQL_INSERT_TASK_LIST,[self.start,self.end,i])
                if(i%10 == 0):
                    connection.commit()
            connection.commit()
            self.logger.info("[MySql]--插入任务数据完毕")


        except Exception as e:
            self.logger.error(e)
        finally:
            if(cursor != None ):
                cursor.close()
            if(connection != None):
                connection.close()

    def generate_redis_list(self):
        """
        构建任务队列
        @return:
        """

        self.logger.info("[Redis]--构建Redis任务队列,日期[{}],数目[{}]".format(self.start,self.totalPage))
        try:
            redis_client = redis.Redis(connection_pool=self.redis_connection_pool)
            for i in range(self.totalPage):
                redis_client.lpush(self.redis_list_name,i)
            self.logger.info("[Redis]--Redis任务队列构建成功")
        except Exception as e:
            self.logger.error(e)




if __name__ == "__main__":
    make_list = makeList('2018-02-01','2018-02-01',100)
    make_list.generate_mysql_list()
    make_list.generate_redis_list()


