Pandahouse
==========

> Note: this is a fork. You probably want to use https://github.com/kszucs/pandahouse

Pandas interface for ClickHouse HTTP API

Install
-------

```bash
pip install pandahouse
```

Usage
-----

Writing a dataframe to ClickHouse

```python
connection = {"host": "http://clickhouse-host:8123",
              "database": "test"}
affected_rows = to_clickhouse(df, table="name", connection=connection)
```

Reading arbitrary ClickHouse query to pandas

```python
df = read_clickhouse("SELECT * FROM {db}.table", index_col="id",
                     connection=connection)
```