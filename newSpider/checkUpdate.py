# 检查指定日期是否有更新的专利
# 如果有，那么根据日期和专利更新的页数 创建专利目录任务表 并 把主键写入Redis中 并 把更新的日期和页数写入更新日期记录表中

import logging
import requests
import math
import time
from bs4 import BeautifulSoup

from newSpider.KeenDBUtill import KeenDBUtill


class checkUpdate(object):
    def __init__(self, date):
        self.date = date
        self.page = -1
        self.cnt = -1
        self.session = requests.session()

        # 日志
        logging.basicConfig(level=logging.INFO, filename="checkUpdate.log", filemode='a',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.StreamHandler())

        # 数据库
        self.mysql_connection_pool = KeenDBUtill.get_connection_pool()

    def get_cookie(self):
        headers = {
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
        self.logger.info("更新cookie")
        u_search = "http://search.beijingip.cn/search/search/advance?t=0"
        self.session = requests.session()
        for i in range(5):
            try:
                response_cookie = self.session.get(url=u_search, headers=headers, timeout=15)
                if response_cookie.status_code == 200:
                    self.logger.info("更新cookie成功")
                    self.logger.info(requests.utils.dict_from_cookiejar(response_cookie.cookies))
                    self.is_cookie = True
                    return
            except Exception as e:
                self.logger.error("第{}次访问失败".format(i))
                self.logger.error(e)
        self.logger.error("获取cookie失败，网站又抽风了")

    def get_response(self):
        headers = {
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
        params = (
            ('s', '(((PD>={} AND (PD<={}))) AND (AC=\u4E2D\u56FD)'.format(self.date, self.date)),
        )

        data = [
            ('searchstrbak', ''),
            ('searchstr', '((PD>={}) AND (PD<={}))'.format(self.date, self.date)),
            ('guojialist', ''),
            ('saveexp', '1'),
            ('ifzhineng', '1'),
            ('matchType', 'on'),
            ('MOL', ''),
            ('ck_ti_id', 'TI'),
            ('as_position', 'AB'),
            ('as_position', 'CLM'),
            ('validity', ''),
            ('validity', ''),
            ('LSDS', ''),
            ('LSDE', ''),
        ]

        logging.info("检查日期[{}]".format(self.date))
        response = self.session.post('http://search.beijingip.cn/search/search/result',
                                     headers=headers,
                                     params=params,
                                     cookies=self.session.cookies,
                                     data=data,
                                     timeout=10,
                                     verify=False)

        return response

    def check_is_update(self, response):
        if response is None:
            return False

        # 如果显示代表专利信息的li标签的数目少于10个，说明该页没有信息
        soup = BeautifulSoup(response.content)
        div_result = soup.find("div", id='searchresult_v')
        lis = div_result.ul.find_all('li', recursive=False)
        if (len(lis) < 10):
            return

        for li in lis:
            name = li.find('a', attrs={'name': 'v_TI'}).text
            self.logger.debug(name)
        num = soup.find('span', id='searchTotalCount').text
        if num != '':
            self.cnt = int(num)
            self.page = math.ceil(self.cnt / 50)

    def sava_update_time(self):
        SQL_INSERT_PATENT_UPDATE_DATE = """
        insert into
            update_record
        values 
            (NULL,%s,%s,%s)
        """

        connection = None
        cursor = None

        try:
            connection = self.mysql_connection_pool.connection()
            cursor = connection.cursor()
            cursor.execute(SQL_INSERT_PATENT_UPDATE_DATE, (self.date, self.page, self.cnt))
            connection.commit()
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def create_mysql_list_table(self):
        pass

    def create_redis_list_key(self):
        pass

    def dispatchor(self):
        """
        专利更新爬取的调度模块
        @return:
        """

        # 获取响应
        for i in range(5):
            self.logger.info("第[{}]次尝试，日期为[{}]".format(i,self.date))
            try:
                self.get_cookie()
                response = self.get_response()
                self.check_is_update(response)

                if self.cnt != -1 and self.page != -1:
                    self.logger.info("第[{}]次尝试成功,日期为[{}],更新了[{}]条".format(i,self.date,self.cnt))
                    # 更新记录保存
                    self.sava_update_time()
                    # 创建目录页任务表
                    self.create_mysql_list_table()
                    # 创建Redis消息队列
                    self.create_redis_list_key()
                    break
                else:
                    self.logger.info("第[{}]次尝试失败,日期为[{}]".format(i,self.date))

                time.sleep(10)

            except Exception as e:
                self.logger.error("第[{}]次尝试失败".format(i))
                self.logger.error(e)


if __name__ == "__main__":
    check_update = checkUpdate('20200101')
    check_update.dispatchor()