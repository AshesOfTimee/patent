#检查某个表下的某个记录是否正确
from newSpider.responseParser import  responseParser
from newSpider.CONSTANT import TABLE_NAME_PREFIX
from newSpider.KeenDBUtill import KeenDBUtill

class checkUtil(object):
    def __init__(self):

        self.parser = responseParser(is_print = True)
        self.mysql_connection_pool = KeenDBUtill.get_connection_pool()

    def check(self,table_name,primary_key):
        SQL_SELECT_CONTENT = """
            SELECT 
                CONTENT 
            FROM 
                `{}`
            WHERE 
                id = %s
        """.format(TABLE_NAME_PREFIX.format(table_name))

        connection = self.mysql_connection_pool.connection()
        cursor = connection.cursor()

        #取出content
        cursor.execute(SQL_SELECT_CONTENT,(primary_key,))
        content = cursor.fetchone()[0].decode('utf-8')

        self.parser.parse(content)

        cursor.close()
        connection.close()

if __name__ == '__main__':
    check_util = checkUtil()
    check_util.check('2019-01-01',90)
