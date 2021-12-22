# -*- coding: utf-8 -*-
"""
Created on 2021-12-04 14:42
---------
@summary: 操作pgsql数据库
---------
@author: 沈瑞祥
@email: ruixiang.shen@outlook.com
"""
from feapder_pipelines.db.pgsqldb import PgsqlDB


db = PgsqlDB(
    ip="localhost", port=5432, db="postgres", user_name="postgres", user_pass="123456"
)

# postgresql://user_name:user_passwd@ip:port/db?charset=utf8
PgsqlDB.from_url("postgresql://postgres:123456@localhost:5432/postgres?charset=utf8")
