|Build Status| |Gitential Active Contributors| |Gitential Coding Hours| |Gitential Efficiency|

.. |Gitential Active Contributors| image:: https://api.gitential.com/accounts/6/projects/121/badges/active-contributors.svg
   :alt: Gitential Efficiency
   :target: https://gitential.com/accounts/6/projects/121/share?uuid=4fc48389-c5e6-48c5-91d8-d11adcdd9405&utm_source=shield&utm_medium=shield&utm_campaign=121

.. |Gitential Coding Hours| image:: https://api.gitential.com/accounts/6/projects/121/badges/coding-hours.svg
   :alt: Gitential Efficiency
   :target: https://gitential.com/accounts/6/projects/121/share?uuid=4fc48389-c5e6-48c5-91d8-d11adcdd9405&utm_source=shield&utm_medium=shield&utm_campaign=121

.. |Gitential Efficiency| image:: https://api.gitential.com/accounts/6/projects/121/badges/efficiency.svg
   :alt: Gitential Efficiency
   :target: https://gitential.com/accounts/6/projects/121/share?uuid=4fc48389-c5e6-48c5-91d8-d11adcdd9405&utm_source=shield&utm_medium=shield&utm_campaign=121

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
                  'database': 'test'
    affected_rows = to_clickhouse(df, table='name', connection=connection)


Reading arbitrary clickhouse query to pandas

.. code:: python

    df = read_clickhouse('SELECT * FROM {db}.table', index_col='id',
                         connection=connection)


.. |Build Status| image:: http://drone.lensa.com:8000/api/badges/kszucs/pandahouse/status.svg
   :target: http://drone.lensa.com:8000/kszucs/pandahouse
