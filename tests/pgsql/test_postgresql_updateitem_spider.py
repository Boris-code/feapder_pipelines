# -*- coding: utf-8 -*-
"""
Created on 2021-12-04 14:42
---------
@summary: 操作pgsql数据库
---------
@author: 沈瑞祥
@email: ruixiang.shen@outlook.com
"""
import feapder
from feapder import Item, UpdateItem


class TestPostgreSQL(feapder.AirSpider):
    __custom_setting__ = dict(
        ITEM_PIPELINES=["feapder_pipelines.pipelines.pgsql_pipeline.PgsqlPipeline"],
        PGSQL_IP="localhost",
        PGSQL_PORT=5432,
        PGSQL_DB="feapder",
        PGSQL_USER_NAME="postgres",
        PGSQL_USER_PASS="123456",
    )

    def start_requests(self):
        yield feapder.Request("https://www.baidu.com")

    def parse(self, request, response):
        title = response.xpath("//title/text()").extract_first()  # 取标题
        for i in range(10):
            item = UpdateItem()  # 声明一个item
            item.table_name = "test_postgresql"
            item.id = i
            item.title = title + str(666)  # 给item属性赋值
            item.index = i
            item.c = "postgresql测试成功"
            yield item  # 返回item， item会自动批量入库


if __name__ == "__main__":
    TestPostgreSQL().start()
