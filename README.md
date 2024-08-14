# Pandas-to-postgres
## by the Growth Lab at Harvard's Center for International Development
This package is part of Harvard Growth Lab’s portfolio of software packages, digital products and interactive data visualizations. To browse our entire portfolio, please visit growthlab.app. To learn more about our research, please visit [Harvard Growth Lab’s home page](https://growthlab.cid.harvard.edu/).

# About
Pandas-to-postgres allows you to bulk load the contents of large dataframes into postgres as quickly as possible. The main differences from pandas' `to_sql` function are:

- Uses `COPY` combined with `to_csv` instead of `execute / executemany`, which runs much faster for large volumes of data
- Uses `COPY FROM STDIN` with `StringIO` to avoid IO overhead to intermediate files. This matters in particular for data stored in unusual formats like HDF, STATA, parquet - common in the scientific world.
- Chunked loading methods to be able to load larger-than-memory tables. In particular the HDF5 functions load data in chunks directly from the file, easily extendible to other formats that support random access by row range.
- Removes indexing overhead by automatically detecting and dropping indexes before load, and then re-creating them afterwards
- Allows you to load multiple separate HDF tables in parallel using multiprocessing.Pool
- Works around pandas null value representation issues: float pandas columns that have an integer SQL type get converted into an object column with int values where applicable and NaN elsewhere.
- Provides hooks to modify data as it's loaded

Anecdotally, we use this to load approximately 640 million rows of data from a 7.1GB HDF file (zlib compressed), 75% of it spread across 3 of 23 tables, with a mean number of columns of 6. We load this into an m4.xlarge RDS instance running postgres 10.3 in 54 minutes (approximately 10-15 minutes of which is recreating indexes), using 4 threads.

# Dependencies

- Python 3
- psycopg2 (for the low level COPY from stdin)
- sqlalchemy (for reflection for indexes)
- pandas
- pyarrow (for copying Parquet files)

# Usage Example

```python3
from pandas_to_postgres import (
    DataFrameCopy,
    hdf_to_postgres,
)

table_model = db.metadata.tables['my_awesome_table']

# already loaded DataFrame & SQLAlchemy Table model
with db.engine.connect() as c:
  DataFrameCopy(df, conn=c, table_obj=table_model).copy()

# HDF from file
hdf_to_postgres('./data.h5', engine_args=["psycopg://..."])

# Parallel HDF from file
hdf_to_postgres('./data.h5', engine_args=["psycopg://..."], processes=4)

# Parquet file
with db.engine.connect() as c:
  ParquetCopy("/path/to/file.parquet", conn=c, table_obj=table_model).copy()

```

# Other Comparisons
- [Odo](http://odo.pydata.org/): A much more general tool that provides some similar features across many formats and databases, but missing a lot of our specific features. Unfortunately currently buggy and unmaintained.
- [Postgres Binary Parser](https://github.com/spitz-dan-l/postgres-binary-parser): Uses `COPY WITH BINARY` to remove the pandas to csv bottleneck, but didn't provide as good an improvement for us.
- [pg_bulkload](https://github.com/ossc-db/pg_bulkload): The industry standard, has some overlap with us. Works extremely well if you have CSV files, but not if you have any other format (you'd have to write your own chunked read/write code and pipe it through, at which point you might as well use ours). Judging by benchmarks we're in the same ballpark. Could perhaps replace psycopg2 as our backend eventually.
