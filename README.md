# FEAPDER 管道扩展

![](https://img.shields.io/badge/python-3-brightgreen)
![](https://img.shields.io/github/watchers/Boris-code/feapder_pipelines?style=social)
![](https://img.shields.io/github/stars/Boris-code/feapder_pipelines?style=social)
![](https://img.shields.io/github/forks/Boris-code/feapder_pipelines?style=social)

## 简介

此模块为`feapder`的`pipelines`扩展，感谢广大开发者对`feapder`的贡献

随着feapder支持的pipelines越来越多，为减少feapder的体积，特将pipelines提出，使用者可按需安装

## 管道

### PostgreSQL

> 贡献者：沈瑞祥
>
> 联系方式：ruixiang.shen@outlook.com


#### 安装 

```
pip install feapder_pipelines[pgsql]
```

#### 使用

在`feapder`项目的`setting.py`中使用如下配置

```python
# PostgreSQL
PGSQL_IP = 
PGSQL_PORT = 
PGSQL_DB = 
PGSQL_USER_NAME = 
PGSQL_USER_PASS = 

ITEM_PIPELINES = [
    "feapder_pipelines.pipelines.pgsql_pipeline.PgsqlPipeline"
]
```


<details>
<summary>细节</summary>
注：入库时 ON CONFLICT(key) 默认为id或通过如下sql查出来的第一个值

```sql
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
    where table_name = 'table_name';
```
</details>


