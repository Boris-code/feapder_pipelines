# -*- coding: utf-8 -*-
"""
Created on 2021-12-04 14:42
---------
@summary: 操作pgsql数据库
---------
@author: 沈瑞祥
@email: ruixiang.shen@outlook.com
"""

from feapder.utils.tools import list2str, format_sql_value


# PostgreSQL数据库相关
def get_indexes_col_sql(table):
    """
    @summary: 适用于PostgreSQL
    ---------
    @param table:

    ---------
    @result:
    """
    sql = """
    select column_names from(
        select
            t.relname as table_name,
            i.relname as index_name,
            array_to_string(array_agg(a.attname), ', ') as column_names
        from
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        where
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            and t.relname like '%'
        group by
            t.relname,
            i.relname
        order by
            t.relname,
            i.relname) as res
    where table_name = '{table}';
    """
    sql = sql.format(table=table).replace("None", "null")
    return sql


def get_primaryKey_col_sql(table):
    """
    @summary: 适用于PostgreSQL
    ---------
    @param table:

    ---------
    @result:
    """
    sql = """
    SELECT
        string_agg(DISTINCT t3.attname,',') AS primaryKeyColumn
        ,t4.tablename AS tableName
        , string_agg(cast(obj_description(relfilenode,'pg_class') as varchar),'') as comment
    FROM
        pg_constraint t1
        INNER JOIN pg_class t2 ON t1.conrelid = t2.oid
        INNER JOIN pg_attribute t3 ON t3.attrelid = t2.oid AND array_position(t1.conkey,t3.attnum) is not null
        INNER JOIN pg_tables t4 on t4.tablename = t2.relname
        INNER JOIN pg_index t5 ON t5.indrelid = t2.oid AND t3.attnum = ANY (t5.indkey)
        LEFT JOIN pg_description t6 on t6.objoid=t3.attrelid and t6.objsubid=t3.attnum
    WHERE t1.contype = 'p'
        AND length(t3.attname) > 0
        AND t2.oid = '{table}' :: regclass
    group by t4.tablename;
    """
    sql = sql.format(table=table).replace("None", "null")
    return sql


def get_constraint_name_sql(table):
    """
    @summary: 适用于PostgreSQL
    ---------
    @param table:tablename
    ---------
    @result:
    """
    sql = "SELECT indexname FROM pg_indexes WHERE tablename = '{table}' order by indexname"
    sql = sql.format(table=table).replace("None", "null")
    return sql


def make_insert_sql(
        table,
        data,
        auto_update=False,
        update_columns=(),
        insert_ignore=False,
        constraint_name=(),
):
    """
    @summary: 适用于PostgreSQL
    ---------
    @param table:
    @param data: 表数据 json格式
    @param auto_update: 更新所有所有列的开关
    @param update_columns: 需要更新的列 默认全部，当指定值时，auto_update设置无效，当duplicate key冲突时更新指定的列
    @param insert_ignore: 更新策略:数据存在则忽略本条数据
    @param constraint_name: 约束名称，用于唯一性判断，通过alter table <table_name> add constraint <constraint_name> unique(<field1,filed2...>) 创建
    ---------
    @result:
    """

    keys = ["{}".format(key) for key in data.keys()]
    keys = list2str(keys).replace("'", "")

    values = [format_sql_value(value) for value in data.values()]
    values = list2str(values)

    if update_columns:
        if not isinstance(update_columns, (tuple, list)):
            update_columns = [update_columns]
        update_columns_ = ", ".join(
            ["{key}=excluded.{key}".format(key=key) for key in update_columns]
        )
        sql = (
                "insert into {table} {keys} values {values} on conflict ON CONSTRAINT {constraint_name}  DO UPDATE SET %s"
                % update_columns_
        )

    elif auto_update:
        update_all_columns_ = ", ".join(
            ["{key}=excluded.{key}".format(key=key) for key in keys]
        )
        sql = (
                "insert into {table} {keys} values {values} on conflict ON CONSTRAINT {constraint_name}  DO UPDATE SET %s"
                % update_all_columns_
        )
    elif insert_ignore:
        sql = "insert into {table} {keys} values {values} on conflict ON CONSTRAINT {constraint_name} DO NOTHING"
    else:
        sql = "insert into {table} {keys} values {values}"

    sql = sql.format(
        table=table, keys=keys, values=values, constraint_name=constraint_name
    ).replace("None", "null")
    return sql


def make_update_sql(table, data, condition):
    """
    @summary: 适用于PostgreSQL
    ---------
    @param table:
    @param data: 表数据 json格式
    @param condition: where 条件
    ---------
    @result:
    """
    key_values = []

    for key, value in data.items():
        value = format_sql_value(value)
        if isinstance(value, str):
            key_values.append("{}={}".format(key, repr(value)))
        elif value is None:
            key_values.append("{}={}".format(key, "null"))
        else:
            key_values.append("{}={}".format(key, value))

    key_values = ", ".join(key_values)

    sql = "update {table} set {key_values} where {condition}"
    sql = sql.format(table=table, key_values=key_values, condition=condition)
    return sql


def make_batch_sql(
    table,
    datas,
    auto_update=False,
    update_columns=(),
    update_columns_value=(),
    constraint_name=(),
):
    """
    @summary: 生产批量的sql
    ---------
    @param table:
    @param datas: 表数据 [{...}]
    @param auto_update: 使用的是replace into， 为完全覆盖已存在的数据
    @param update_columns: 需要更新的列 默认全部，当指定值时，auto_update设置无效，当duplicate key冲突时更新指定的列
    @param update_columns_value: 需要更新的列的值 默认为datas里边对应的值, 注意 如果值为字符串类型 需要主动加单引号， 如 update_columns_value=("'test'",)
    @param constraint_name: 约束名称，用于唯一性判断，通过alter table <table_name> add constraint <constraint_name> unique(<field1,filed2...>) 创建
    ---------
    @result:
    """
    if not datas:
        return

    keys = list(datas[0].keys())
    values_placeholder = ["%s"] * len(keys)

    values = []
    for data in datas:
        value = []
        for key in keys:
            current_data = data.get(key)
            current_data = format_sql_value(current_data)

            value.append(current_data)

        values.append(value)

    keys = ["{}".format(key) for key in keys]
    keys = list2str(keys).replace("'", "")

    values_placeholder = list2str(values_placeholder).replace("'", "")

    if update_columns:
        if not isinstance(update_columns, (tuple, list)):
            update_columns = [update_columns]
        if update_columns_value:
            update_columns_ = ", ".join(
                [
                    "{key}=excluded.{value}".format(key=key, value=value)
                    for key, value in zip(update_columns, update_columns_value)
                ]
            )
        else:
            update_columns_ = ", ".join(
                ["{key}=excluded.{key}".format(key=key) for key in update_columns]
            )
        # sql = "insert into {table} {keys} values {values_placeholder} ON CONFLICT({indexes_cols}) DO UPDATE SET {update_columns}".format(
        sql = "insert into {table} {keys} values {values_placeholder} on conflict ON CONSTRAINT {constraint_name} DO UPDATE SET {update_columns}".format(
            table=table,
            keys=keys,
            values_placeholder=values_placeholder,
            update_columns=update_columns_,
            constraint_name=constraint_name,
        )
    elif auto_update:
        update_all_columns_ = ", ".join(
            ["{key}=excluded.{key}".format(key=key) for key in keys]
        )
        # sql = "insert into {table} {keys} values {values_placeholder} on conflict({indexes_cols}) DO UPDATE SET {update_all_columns_}".format(
        sql = "insert into {table} {keys} values {values_placeholder} on conflict ON CONSTRAINT {constraint_name} DO UPDATE SET {update_all_columns_}".format(
            table=table,
            keys=keys,
            values_placeholder=values_placeholder,
            constraint_name=constraint_name,
            update_all_columns_=update_all_columns_,
        )
    else:
        # sql = "insert into {table} {keys} values {values_placeholder} on conflict({indexes_cols}) do nothing".format(
        sql = "insert into {table} {keys} values {values_placeholder} on conflict ON CONSTRAINT {constraint_name} do nothing".format(
            table=table,
            keys=keys,
            values_placeholder=values_placeholder,
            constraint_name=constraint_name,
        )

    return sql, values

