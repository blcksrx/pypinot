Pypinot
=======
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/apache/incubator-superset.svg?branch=master)](https://travis-ci.org/blcksrx/pypinot.svg?branch=master)
[![PyPI version](https://badge.fury.io/py/pypinot.svg)](https://badge.fury.io/py/pypinot)

# Python DB-API and SQLAlchemy dialect for Pinot #

[Apache Pinot](https://pinot.apache.org/) is a realtime distributed OLAP datastore, designed to answer OLAP queries with low latency.
This package provides **DB API** to interact with Apache Pinot

## Installaion ##
```
pip install pypinot
```


## Usage ##

Using the DB API:

```python
from pypinot.connection import Connection

conn = Connection(host='localhost', port=8099, path='/query', scheme='http')
cur = conn.cursor()
cur.execute("""
    SELECT place,
           CAST(REGEXP_EXTRACT(place, '(.*),', 1) AS FLOAT) AS lat,
           CAST(REGEXP_EXTRACT(place, ',(.*)', 1) AS FLOAT) AS lon
      FROM places
     LIMIT 10
""")
for row in cur:
    print(row)
```

## Contribution
Your Contribution is welcome!
      
