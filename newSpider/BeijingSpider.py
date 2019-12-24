import requests
import configparser
from redis import ConnectionPool
import redis
from DBUtils.PooledDB import PooledDB
import  pymysql
from  newSpider.KeenDBUtill import KeenDBUtill

class BeijingSpider (object):

    #爬虫相关类变量
    response = None#响应
    data = {
          'xd_use_expand': 'false',
          'ifsynonymexpand': 'false',
          'ifenterprisenameexpand': 'false',
          'ifnomalize': '0',
          'ifzhineng': '0',
          'searchstr': '(((PD>=20180101) AND (PD<=20181231))) AND (AC=\u4E2D\u56FD)',
          'page': '1',
          'stype': '0',
          'opnum': '1',
          'guojialist': '',
          'perpage': '50',
          'pn': '',
          'mid': '',
          'an': '',
          'num': '',
          #'maxPage': '80180',
          #'cnt': '4008993',
          'idisp_words': '%5B%7B%22phrase%22%3A%22%5Cu4e2d%5Cu56fd%22%2C%22field%22%3A%22AC%22%2C%22subphrase%22%3A%5B%22%5Cu4e2d%5Cu56fd%22%5D%7D%5D',
          'sortby': 'XGD|0',
          'isdownload': '',
          'downloadcurpage': '',
          'downloadstartpage': '',
          'downloadendpage': '',
          'downloadfields': '',
          'downloadtype': '',
          'downloadtypelist': '',
          'saveexp': '0',
          'addedCount': '0',
          'addedValue': ' ',
          'addedTitle': ' ',
          'addedFolderName': '',
          'leftFilterShow': '',
          'leftFilterQuery': '',
          'centerFilterQuery': '',
          'centerFilterShow': '0',
          'filteredQ': '',
          'leftFilterCurr': '',
          #'originTC': '4008993',
          'customfield': 'TI,LS,PA,AN,AD,PN,PD,AB,ZYFT,AU,IPC,IPCR,PR,ADDR,PC,AGC,AGT',
          'viewresultzoneonly': '',
          'opkey1': 'ZNJS',
          'opv1': '',
          'op1': 'AND'
        }#POST数据
    params = (
    ('s', '(((PD>=20180101) AND (PD<=20181231))) AND (AC=\u4E2D\u56FD)'),
    )
    u_post = 'http://search.beijingip.cn/search/search/result'

    #Redis相关类变量
    parser = configparser.ConfigParser()
    parser.read("spider.conf")
    redis_host = parser['redis']['host']
    redis_port = parser['redis']['port']




    #连接Redis

    redisPool = ConnectionPool(host=redis_host, port=redis_port, max_connections=10)
    mysqlPool = PooledDB(pymysql, maxconnections=mysql_maxconnections, host=mysql_host, user=mysql_username, passwd=mysql_password, db=mysql_databases, port=mysql_port)



    #爬取的搜索关键字
    def __init__(self):
        self.session = requests.session()
        self.headers = headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Origin': 'http://search.beijingip.cn',
            'Upgrade-Insecure-Requests': '1',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'http://search.beijingip.cn/search/search/result?s=(((PD%3E=20130101)%20AND%20(PD%3C=20131231)))%20AND%20(AC=%E4%B8%AD%E5%9B%BD)',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            }

        # self.mysqlPool = PooledDB(pymysql, maxconnections=BeijingSpider.mysql_maxconnections,
        #                           host=BeijingSpider.mysql_host,
        #                           user=BeijingSpider.mysql_username,
        #                           passwd=BeijingSpider.mysql_password,
        #                           db=BeijingSpider.mysql_databases,
        #                           port=BeijingSpider.mysql_port)

    #模仿用户正常行为路径，在访问数据接口前先访问搜索页面，在该页面上获得cookie
    def get_cookie(self):
        u_search = "http://search.beijingip.cn/search/search/advance?t=0"
        self.session.get(url=u_search,headers=self.headers)
        #print(self.session.cookies)

    def get_response(self):
        #异常捕获与处理
        self.response=self.session.post('http://search.beijingip.cn/search/search/result',
                                        headers=self.headers,
                                        params=self.params,
                                        data=self.data,
                                        verify=False)




    #以下均为开发时候的测试方法，不会正式使用,我懒得再写测试了，看到这里的学弟or学妹可以不用看下面的代码了 ：）
    def print_response(self):
        print(self.response.text)

    def test_redis(self):
        redis_client = redis.Redis(connection_pool= self.redisPool)

        try:
            redis_client.set('a','b')
        except Exception as e:
            print(e)
        finally:
            pass

    def test_mysql(self):
        conn = BeijingSpider.mysqlPool.connection()
        cursor = conn.cursor()
        SQL = "show databases"
        cursor.execute(SQL)
        print(cursor.fetchall())

        cursor.close()
        conn.close()

if __name__ == "__main__":
    spider1 = BeijingSpider()
    spider1.test_mysql()




