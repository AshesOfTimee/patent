# 用bs4解析返回的response

from bs4 import BeautifulSoup
import logging

from newSpider.KeenDBUtill import KeenDBUtill
from newSpider.KeenRedisUtill import KeenRedisUtill


class responseParser(object):

    def __init__(self, is_print=False):
        self.is_print = is_print
        logging.basicConfig(level=logging.INFO, filename="responseParser.log", filemode='a',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.mysql_connection_pool = KeenDBUtill.get_connection_pool()

    def parse(self, content):
        """
        @param is_print: 是否把解析后的结果打印出来
        @return:
        """
        soup = BeautifulSoup(content, 'html.parser')
        div_result = soup.find("div", id='searchresult_v')
        lis = div_result.ul.find_all('li', recursive=False)

        names = []
        for index, li in enumerate(lis):
            names.append(li.find('a', attrs={'name': 'v_TI'}).text.strip())

        if self.is_print:
            for index, name in enumerate(names):
                print(index+1, name)
