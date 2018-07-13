# Pandas-to-postgres

Pandas-to-postgres allows you to bulk load the contents of large dataframes into postgres as quickly as possible. The main differences from pandas' `to_sql` functions are:

- Uses `COPY` combined with `to_csv` instead of `execute / executemany`, which runs much faster for large volumes of data
- Uses `COPY FROM STDIN` with `StringIO` to avoid IO overhead to intermediate files. This matters in particular for data stored in unusual formats like HDF, STATA, parquet - common in the scientific world.
- Chunked loading methods to be able to load larger-than-memory tables. In particular the HDF5 functions load data in chunks directly from the file, easily extendible to other formats that support random access by row range.
- Removes indexing overhead by automatically detecting and dropping indexes before load, and then re-creating them afterwards
- Loads separate tables in parallel using multiprocessing
- Hooks to modify data as it's loaded 

# Dependencies 

- Python 3
- psycopg2 (for the low level COPY from stdin)
- sqlalchemy (for reflection for indexes)
- pandas

# Usage Example

```python3

from pandas_to_postgres import ...

# already loaded dataframe
...

# HDF from file
...

```
