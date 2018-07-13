from typing import List
from multiprocessing import Pool
import SQLAlchemy
from pandas_to_postgres import HDFTableCopy, HDFMetadata


def create_hdf_table_objects(hdf_meta, csv_chunksize=10 ** 6):
    tables = []

    for sql_table, hdf_tables in hdf_meta.sql_to_hdf.items():
        tables.append(
            HDFTableCopy(
                hdf_tables,
                hdf_meta,
                defer_sql_objs=True,
                sql_table=sql_table,
                csv_chunksize=csv_chunksize,
            )
        )

    return tables


def copy_worker(db: SQLAlchemy, copy_obj: HDFTableCopy, defer_sql_objs: bool = True):
    db.engine.dispose()
    with db.engine.connect() as conn:
        conn.execution_options(autocommit=True)
        conn.execute("SET maintenance_work_mem TO 1000000;")

        if defer_sql_objs:
            table_obj = db.metadata.tables[copy_obj.sql_table]
            copy_obj.instantiate_sql_objs(conn, table_obj)

        copy_obj.copy()


def hdf_to_postgres(
    file_name: str, db: SQLAlchemy, keys: List[str] = [], csv_chunksize: int = 10 ** 6
):
    """
    Copy tables in a HDF file to PostgreSQL database

    Parameters
    ----------
    file_name: name of file or path to file of HDF to use to copy
    db: SQLAlchemy object of destination database
    keys: list of HDF keys to copy
    csv_chunksize: maximum number of StringIO CSV rows to keep in memory at a time
    """
    hdf = HDFMetadata(
        file_name, keys, metadata_attr="atlas_metadata", metadata_keys=["levels"]
    )

    tables = create_hdf_table_objects(hdf, csv_chunksize=csv_chunksize)

    for table in tables:
        copy_worker(table, defer_sql_objs=True)


def multiprocess_hdf_to_postgres(
    file_name: str,
    db: SQLAlchemy,
    keys: List[str] = [],
    processes: int = 4,
    csv_chunksize: int = 10 ** 6,
):
    """
    Copy tables in a HDF file to PostgreSQL database using a multiprocessing Pool

    Parameters
    ----------
    file_name: name of file or path to file of HDF to use to copy
    db: SQLAlchemy object of destination database
    keys: list of HDF keys to copy
    processes: number of processes in the Pool
    csv_chunksize: maximum number of StringIO CSV rows to keep in memory at a time
    """

    hdf = HDFMetadata(
        file_name, keys, metadata_attr="atlas_metadata", metadata_keys=["levels"]
    )

    tables = create_hdf_table_objects(hdf, csv_chunksize=csv_chunksize)

    try:
        p = Pool(processes)
        p.map(copy_worker, tables, chunksize=1)
    finally:
        del tables
        del hdf
        p.close()
        p.join()
