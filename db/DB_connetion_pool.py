# -*- coding: UTF-8 -*-
"""
@描述：数据库连接池管理模块
"""
import pymysql;
from DBUtils.PooledDB import PooledDB;

import db.DB_config as Config;

'''
@功能：PCSD数据库连接池
'''
class PCSDConnectionPool(object):
    __pool = None;
    def __enter__(self):
        self.conn = self.getConn();
        self.cursor = self.conn.cursor();
        print ("PCSD数据库创建con和cursor");
        return self

    def getConn(self):
        if self.__pool is None:
            self.__pool = PooledDB(creator=pymysql, mincached=Config.DB_MIN_CACHED , maxcached=Config.DB_MAX_CACHED,
                                   maxshared=Config.DB_MAX_SHARED, maxconnections=Config.DB_MAX_CONNECYIONS,
                                   blocking=Config.DB_BLOCKING, maxusage=Config.DB_MAX_USAGE,
                                   setsession=Config.DB_SET_SESSION,
                                   host=Config.DB_TEST_HOST , port=Config.DB_TEST_PORT ,
                                   user=Config.DB_TEST_USER , passwd=Config.DB_TEST_PASSWORD ,
                                   db=Config.DB_TEST_DBNAME , use_unicode=False, charset=Config.DB_CHARSET);

        return self.__pool.connection()

    """
    @summary: 释放连接池资源
    """
    def __exit__(self, type, value, trace):
        self.cursor.close()
        self.conn.close()

        print ("PCSD连接池释放con和cursor");

'''
@功能：获取PCSD数据库连接
'''
def getPCSDConnection():
    return PCSDConnectionPool();