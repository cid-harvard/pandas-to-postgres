from multiprocessing import Pool

from .copy_hdf import HDFTableCopy
from .utilities import HDFMetadata


def create_hdf_table_objects(hdf_meta, csv_chunksize=10 ** 6):
    """
    Create list of HDFTableCopy objects to iterate to run the copy method on each

    Parameters
    ----------
    hdf_meta: HDFMetadata
        Object built from HDF metadata to reference in each copier
    csv_chunksize: int
        Maximum number of rows to store in an in-memory StringIO CSV

    Returns
    -------
    tables: list
        List of HDFTableCopy objects
    """
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


def _copy_worker(copy_obj, defer_sql_objs=True):
    """
    Handle a SQLAlchemy connection and copy using HDFTableCopy object

    copy_obj: HDFTableCopy or subclass
        Object to use to run the copy() method on
    defer_sql_objs: bool
        If True, SQL objects were not build upon instantiation of copy_obj and should
        be built before copying data to db (needed for multiprocessing)
    """
    database.engine.dispose()
    with database.engine.connect() as conn:
        conn.execution_options(autocommit=True)
        conn.execute("SET maintenance_work_mem TO 1000000;")

        if defer_sql_objs:
            table_obj = database.metadata.tables[copy_obj.sql_table]
            copy_obj.instantiate_sql_objs(conn, table_obj)

        copy_obj.copy()


def hdf_to_postgres(file_name, db, keys=[], csv_chunksize=10 ** 6):
    """
    Copy tables in a HDF file to PostgreSQL database

    Parameters
    ----------
    file_name: str
        name of file or path to file of HDF to use to copy
    db: SQLAlchemy database object
        destination database
    keys: list of strings
        HDF keys to copy
    csv_chunksize: int
        Maximum number of StringIO CSV rows to keep in memory at a time
    """

    global database
    database = db

    hdf = HDFMetadata(
        file_name, keys, metadata_attr="atlas_metadata", metadata_keys=["levels"]
    )

    tables = create_hdf_table_objects(hdf, csv_chunksize=csv_chunksize)

    for table in tables:
        _copy_worker(table, defer_sql_objs=True)


def multiprocess_hdf_to_postgres(
    file_name, db, keys=[], processes=4, csv_chunksize=10 ** 6
):
    """
    Copy tables in a HDF file to PostgreSQL database using a multiprocessing Pool

    Parameters
    ----------
    file_name: str
        Name of file or path to file of HDF to use to copy
    db: SQLAlchemy object
        Destination database
    keys: list of strings
        HDF keys to copy
    processes: int
        Number of processes in the Pool
    csv_chunksize: int
        Maximum number of StringIO CSV rows to keep in memory at a time
    """

    global database
    database = db

    hdf = HDFMetadata(
        file_name, keys, metadata_attr="atlas_metadata", metadata_keys=["levels"]
    )

    tables = create_hdf_table_objects(hdf, csv_chunksize=csv_chunksize)

    try:
        p = Pool(processes)
        p.map(_copy_worker, tables, chunksize=1)
    finally:
        del tables
        del hdf
        p.close()
        p.join()
