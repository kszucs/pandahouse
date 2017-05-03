|Build Status|

Pandahouse
==========

Pandas interface for Clickhouse HTTP API


Install
-------

```bash
pip install pandahouse
```

Usage
-----

Given the addressbook protobuf definition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Writing

.. code:: python

    affected_rows = to_clickhouse(df, table='name',
                                  database='test', host=host)


Reading

.. code:: python
    df = read_clickhouse('SELECT * FROM {db}.table', index_col='id',
                         database='test', host=host)


.. |Build Status| image:: http://drone.lensa.com:8000/api/badges/kszucs/pandahouse/status.svg
   :target: http://drone.lensa.com:8000/kszucs/pandahouse
