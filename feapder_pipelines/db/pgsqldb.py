# -*- coding: utf-8 -*-
"""
Created on 2021-12-04 14:42
---------
@summary: 操作pgsql数据库
---------
@author: 沈瑞祥
@email: ruixiang.shen@outlook.com
"""

from typing import List, Dict
from urllib import parse

import psycopg2
from dbutils.pooled_db import PooledDB

import feapder.setting as setting
from feapder.db.mysqldb import MysqlDB
from feapder.utils.log import log
from feapder_pipelines.utils.pgsql_tool import (
    make_insert_sql,
    make_update_sql,
    make_batch_sql,
)


def auto_retry(func):
    def wapper(*args, **kwargs):
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                log.error(
                    """
                    error:%s
                    sql:  %s
                    """
                    % (e, kwargs.get("sql") or args[1])
                )

    return wapper


class PgsqlDB(MysqlDB):
    def __init__(
        self, ip=None, port=None, db=None, user_name=None, user_pass=None, **kwargs
    ):
        # 可能会改setting中的值，所以此处不能直接赋值为默认值，需要后加载赋值
        if not ip:
            ip = setting.PGSQL_IP
        if not port:
            port = setting.PGSQL_PORT
        if not db:
            db = setting.PGSQL_DB
        if not user_name:
            user_name = setting.PGSQL_USER_NAME
        if not user_pass:
            user_pass = setting.PGSQL_USER_PASS

        try:
            self.connect_pool = PooledDB(
                creator=psycopg2,
                mincached=1,
                maxcached=100,
                maxconnections=100,
                blocking=True,
                ping=7,
                host=ip,
                port=port,
                user=user_name,
                password=user_pass,
                database=db,
            )

        except Exception as e:
            log.error(
                """
            连接失败：
            ip: {}
            port: {}
            db: {}
            user_name: {}
            user_pass: {}
            exception: {}
            """.format(
                    ip, port, db, user_name, user_pass, e
                )
            )
        else:
            log.debug("连接到postgresql数据库 %s : %s" % (ip, db))

    @classmethod
    def from_url(cls, url, **kwargs):
        # postgresql://user_name:user_passwd@ip:port/db?charset=utf8
        url_parsed = parse.urlparse(url)

        db_type = url_parsed.scheme.strip()
        if db_type != "postgresql":
            raise Exception(
                "url error, expect postgresql://username:ip:port/db?charset=utf8, but get {}".format(
                    url
                )
            )

        connect_params = {
            "ip": url_parsed.hostname.strip(),
            "port": url_parsed.port,
            "user_name": url_parsed.username.strip(),
            "user_pass": url_parsed.password.strip(),
            "db": url_parsed.path.strip("/").strip(),
        }

        connect_params.update(kwargs)

        return cls(**connect_params)

    def add_smart(self, table, data: Dict, **kwargs):
        """
        添加数据, 直接传递json格式的数据，不用拼sql
        Args:
            table: 表名
            data: 字典 {"xxx":"xxx"}
            **kwargs:

        Returns: 添加行数

        """
        sql = make_insert_sql(table, data, **kwargs)
        return self.add(sql)

    def add_batch(self, sql, datas: List[Dict]):
        """
        @summary: 批量添加数据
        ---------
        @ param sql: insert into (xxx,xxx) values (%s, %s, %s)
        # param datas: 列表 [{}, {}, {}]
        ---------
        @result: 添加行数
        """
        try:
            conn, cursor = self.get_connection()
            cursor.executemany(sql, datas)
            affect_count = cursor.rowcount
            conn.commit()

        except Exception as e:
            log.error(
                """
                error:%s
                sql:  %s
                """
                % (e, sql)
            )
            affect_count = None
        finally:
            self.close_connection(conn, cursor)

        return affect_count

    def add_batch_smart(self, table, datas: List[Dict], **kwargs):
        """
        批量添加数据, 直接传递list格式的数据，不用拼sql
        Args:
            table: 表名
            datas: 列表 [{}, {}, {}]
            **kwargs:

        Returns: 添加行数

        """
        sql, datas = make_batch_sql(table, datas, **kwargs)
        return self.add_batch(sql, datas)

    def update_smart(self, table, data: Dict, condition):
        """
        更新, 不用拼sql
        Args:
            table: 表名
            data: 数据 {"xxx":"xxx"}
            condition: 更新条件 where后面的条件，如 condition='status=1'

        Returns: True / False

        """
        sql = make_update_sql(table, data, condition)
        return self.update(sql)
