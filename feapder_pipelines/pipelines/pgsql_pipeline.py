# -*- coding: utf-8 -*-
"""
Created on 2021-12-04 14:42
---------
@summary: pgsql pipeline
---------
@author: 沈瑞祥
@email: ruixiang.shen@outlook.com
"""

from typing import Dict, List, Tuple

import feapder_pipelines.utils.pgsql_tool as tools
from feapder_pipelines.db.pgsqldb import PgsqlDB
from feapder.pipelines import BasePipeline
from feapder.utils.log import log


class PgsqlPipeline(BasePipeline):
    def __init__(self):
        self._to_db = None
        self._indexes_cols_cached = {}

    @property
    def to_db(self):
        if not self._to_db:
            self._to_db = PgsqlDB()

        return self._to_db

    def __get_indexes_cols(self, table):
        if table not in self._indexes_cols_cached:
            get_indexes_sql = tools.get_primaryKey_col_sql(table)
            indexes_cols = self.to_db.find(sql=get_indexes_sql) or "id"
            log.info(f"主键列名:{indexes_cols[0][0]}")
            if indexes_cols:
                indexes_cols = indexes_cols[0][0]
            self._indexes_cols_cached[table] = indexes_cols

        return self._indexes_cols_cached[table]

    def save_items(self, table, items: List[Dict]) -> bool:
        """
        保存数据
        Args:
            table: 表名
            items: 数据，[{},{},...]

        Returns: 是否保存成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """
        sql, datas = tools.make_batch_sql(
            table, items, indexes_cols=self.__get_indexes_cols(table)
        )
        add_count = self.to_db.add_batch(sql, datas)
        # log.info(sql)
        datas_size = len(datas)
        if add_count is not None:
            log.info(
                "共导出 %s 条数据 到 %s, 重复 %s 条" % (datas_size, table, datas_size - add_count)
            )

        return add_count is not None

    def update_items(self, table, items: List[Dict], update_keys=Tuple) -> bool:
        """
        更新数据
        Args:
            table: 表名
            items: 数据，[{},{},...]
            update_keys: 更新的字段, 如 ("title", "publish_time")

        Returns: 是否更新成功 True / False
                 若False，不会将本批数据入到去重库，以便再次入库

        """
        sql, datas = tools.make_batch_sql(
            table,
            items,
            update_columns=update_keys or list(items[0].keys()),
            indexes_cols=self.__get_indexes_cols(table),
        )
        # log.info(sql)
        update_count = self.to_db.add_batch(sql, datas)
        if update_count:
            msg = "共更新 %s 条数据 到 %s" % (update_count, table)
            if update_keys:
                msg += " 更新字段为 {}".format(update_keys)
            log.info(msg)

        return update_count is not None
