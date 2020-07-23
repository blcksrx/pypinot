# Python DB-API and SQLAlchemy dialect for Pinot #

[Apache Pinot](https://pinot.apache.org/) is a realtime distributed OLAP datastore, designed to answer OLAP queries with low latency.
This package provides **DB API** to interact with Apache Pinot

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
      
