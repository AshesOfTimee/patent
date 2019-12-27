import configparser
from redis import ConnectionPool
import redis


class KeenRedisUtill(object):

    parser = configparser.ConfigParser()
    parser.read("redis.conf")
    redis_host = parser['redis']['host']
    redis_port = parser['redis']['port']
    redisPool = ConnectionPool(host=redis_host, port=redis_port, max_connections=10)

    @classmethod
    def get_connection_pool(cls):
        return cls.redisPool