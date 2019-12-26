import requests
import configparser
import redis
import logging
import fake_useragent
import os
from bs4 import BeautifulSoup

from newSpider.KeenDBUtill import KeenDBUtill
from newSpider.KeenRedisUtill import KeenRedisUtill
from newSpider.CONSTANT import *


class BeijingSpider(object):
    # Redis相关类变量
    parser = configparser.ConfigParser()
    parser.read("spider.conf")

    # 爬取的搜索关键字
    def __init__(self, start, end):
        """
        @param start: 需要爬取的日期。该日期是Redis里任务队列的Key，也是MYSQL里任务队列的表名称。
        @param end:默认情况下end和start相等，代表检索一天的数据。如果start end不相等代表检索时间区间的数据。
        """
        self.start = start
        self.end = end
        search_start = start.replace('-', '')
        search_end = end.replace('-', '')

        # 返回的结果是否正确
        self.flag = False

        # 日志
        logging.basicConfig(level=logging.INFO, filename="beijingSpider.log", filemode='a',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # MYSQL Redis 连接池
        self.mysql_connection_pool = KeenDBUtill.get_connection_pool()
        self.redis_connection_pool = KeenRedisUtill.get_connection_pool()
        self.redis_client = redis.Redis(connection_pool=KeenRedisUtill.get_connection_pool())

        # mysql表名称
        self.mysql_table_name = TABLE_NAME_PREFIX.format(self.start)

        # Redis列表名称
        self.redis_list_name = REDIS_PREFIX + self.start

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
        location = os.getcwd() + '/fake_useragent.json'
        self.fake_userAgent = fake_useragent.UserAgent(path=location)

        # 设置data param
        self.data = {
            'xd_use_expand': 'false',
            'ifsynonymexpand': 'false',
            'ifenterprisenameexpand': 'false',
            'ifnomalize': '0',
            'ifzhineng': '0',
            'searchstr': '(((PD>={}) AND (PD<={}))) AND (AC=\u4E2D\u56FD)'.format(search_start, search_end),  # 检索区间
            'page': '1',
            'stype': '0',
            'opnum': '1',
            'guojialist': '',
            'perpage': '50',
            'pn': '',
            'mid': '',
            'an': '',
            'num': '',
            'maxPage': '80180',  # 专利多少页，需要设置
            'cnt': '4008993',  # 专利多少个，需要设置
            'idisp_words': '',
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
            'originTC': '4008993',  # 专利多少个，需要设置
            'customfield': 'TI,LS,PA,AN,AD,PN,PD,AB,ZYFT,AU,IPC,IPCR,PR,ADDR,PC,AGC,AGT',
            'viewresultzoneonly': '',
            'opkey1': 'ZNJS',
            'opv1': '',
            'op1': 'AND'
        }  # POST数据

        searchstr = '(((PD>={}) AND (PD<={})))'.format(search_start, search_end)
        try:
            redis_client = redis.Redis(connection_pool=self.redis_connection_pool)
            totalCnt = int(redis_client.get(REDIS_PREFIX_CNT + self.start))
            cnt = totalCnt
            originTC = totalCnt
            maxPage = totalCnt / 50

            self.data['searchstr'] = searchstr
            self.data['maxPage'] = maxPage
            self.data['cnt'] = cnt
            self.data['originTC'] = originTC
            # self.params = ('s', '(((PD>=) AND (PD<={}))) AND (AC=\u4E2D\u56FD)'.format(self.start, self.end))
            self.params = (
                ('s', '(((PD>={} AND (PD<={}))) AND (AC=\u4E2D\u56FD)'.format(search_start, search_end)),
            )

        except Exception as e:
            self.logger.error(e)

    # 模仿用户正常行为路径，在访问数据接口前先访问搜索页面，在该页面上获得cookie
    # 但是这个页面经常抽风上不去
    def get_cookie(self):
        self.logger.info("更新cookie")
        u_search = "http://search.beijingip.cn/search/search/advance?t=0"
        self.session = requests.session()
        for _ in range(5):
            try:
                response_cookie = self.session.get(url=u_search, headers=self.headers, timeout=15)
                if response_cookie.status_code == 200:
                    return
            except Exception as e:
                self.logger.error(e)
        self.logger.error("获取cookie失败，网站又抽风了")

    def get_response(self, page):
        """
        爬取第page页的数据
        @param page: 需要爬取的页面
        @return:
        """
        self.data['page'] = str(page, encoding='utf-8')
        self.logger.info("=================拼接后的数据=================")
        self.logger.info(self.data)
        self.logger.info(self.params)
        self.logger.info("=================拼接后的数据=================")
        response = self.session.post('http://search.beijingip.cn/search/search/result',
                                     headers=self.headers,
                                     params=self.params,
                                     cookies=self.session.cookies,
                                     data=self.data,
                                     verify=False)

        return response

    def check_response(self, response):
        self.flag = True

    def parse_response(self, response):
        soup = BeautifulSoup(response.content, 'html.parser')
        div_result = soup.find("div", id='searchresult_v')
        lis = div_result.ul.find_all('li', recursive=False)
        for li in lis:
            name = li.find('a', attrs={'name': 'v_TI'}).text

    def update_mysql_content(self, primary_key, response):
        if self.flag:
            SQL_UPDATE_CONTENT = """
                        UPDATE `{}` SET 
                        `content` = %s
                        where id = %s
                        """.format(self.mysql_table_name)

            connection = None
            cursor = None
            try:
                connection = self.mysql_connection_pool.connection()
                cursor = connection.cursor()
                cursor.execute(SQL_UPDATE_CONTENT, (response.content, primary_key))
                connection.commit()


            # except Exception as e:
            #     self.logger.error(e)
            finally:
                if (cursor != None):
                    cursor.close()
                if (connection != None):
                    connection.close()

    def update_redis_detail(self, primary_key):
        self.redis_client.lpop(self.redis_list_name)
        self.redis_client.lpush(REDIS_PREFIX_DETAIL + self.start, primary_key)

    def dispatch(self):
        cnt = 0
        self.logger.info("爬虫[{}]启动".format(self.start))
        while (True):
            # 每隔十次重新更新一次cookie
            if (cnt % 10 == 0):
                self.get_cookie()

            # 从任务队列取出任务
            page = None
            try:
                if self.redis_client.exists(self.redis_list_name):
                    page = self.redis_client.lindex(self.redis_list_name,
                                                    0)  # 取出队列最左侧的数据，这里使用index而非直接Pop是考虑到可能取出来但是爬取失败
                    self.logger.info("[Redis]--获取任务,日期为[{}],第[{}]页".format(self.start, page))

            except Exception as e:
                self.logger.error(e)

            # 如果page不为None说明任务队列里还有任务
            if page != None:
                response = self.get_response(page)
                self.parse_response(response)
                self.check_response(response)
                # 如果爬取成功，更新MySQL的content字段和Redis的任务队列
                if self.flag == True and response != None:
                    self.update_mysql_content(page, response)
                    self.update_redis_detail(page)
            else:
                self.logger.info("日期[{}]已经爬取完毕!".format(self.start))

            cnt += 1
            break


if __name__ == "__main__":
    spider = BeijingSpider('2019-01-01', '2019-01-31')
    spider.dispatch()
