Pandahouse
==========

Pandas interface for Clickhouse HTTP API


Install
-------

.. code:: bash

    pip install pandahouse


Usage
-----

Writing dataframe to clickhouse

.. code:: python

    connection = {'host': 'http://clickhouse-host:8123',
                  'database': 'test'}
    affected_rows = to_clickhouse(df, table='name', connection=connection)


Reading arbitrary clickhouse query to pandas

.. code:: python

    df = read_clickhouse('SELECT * FROM {db}.table', index_col='id',
                         connection=connection)
