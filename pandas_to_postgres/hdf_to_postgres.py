import logging
from multiprocessing import Pool

from sqlalchemy import MetaData, create_engine

from .copy_hdf import HDFTableCopy
from .utilities import HDFMetadata


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s %(message)s",
    datefmt="%Y-%m-%d,%H:%M:%S",
)

logger = logging.getLogger(__name__)


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


def _copy_worker(copy_obj, engine_args, engine_kwargs, maintenance_work_mem="1G"):

    # Since we fork()ed into a new process, the engine contains process
    # specific stuff that shouldn't be shared - this creates a fresh Engine
    # with the same settings but without those.

    engine = create_engine(*engine_args, **engine_kwargs)
    metadata = MetaData(bind=engine)
    metadata.reflect()

    with engine.connect() as conn:

        conn.execution_options(autocommit=True)

        if maintenance_work_mem is not None:
            conn.execute("SET maintenance_work_mem TO {};".format(maintenance_work_mem))

        # Get SQLAlchemy Table object
        table_obj = metadata.tables.get(copy_obj.sql_table, None)
        if table_obj is None:
            raise ValueError("Table {} does not exist.".format(copy_obj.sql_table))

        copy_obj.instantiate_sql_objs(conn, table_obj)

        # Run the task
        copy_obj.copy()


def hdf_to_postgres(file_name, engine_args, engine_kwargs={}, keys=[],
                    csv_chunksize=10 ** 6, processes=None,
                    maintenance_work_mem=None):
    """
    Copy tables in a HDF file to PostgreSQL database

    Parameters
    ----------
    file_name: str
        name of file or path to file of HDF to use to copy
    engine_args: list
        arguments to pass into create_engine()
    engine_kwargs: dict
        keyword arguments to pass into create_engine()
    keys: list of strings
        HDF keys to copy
    csv_chunksize: int
        Maximum number of StringIO CSV rows to keep in memory at a time
    processes: int or None
        If None, run single threaded. If integer, number of processes in the
        multiprocessing Pool
    maintenance_work_mem: str or None
        What to set postgresql's maintenance_work_mem option to: this helps
        when rebuilding large indexes, etc.
    """

    hdf = HDFMetadata(
        file_name, keys, metadata_attr="atlas_metadata", metadata_keys=["levels"]
    )

    tables = create_hdf_table_objects(hdf, csv_chunksize=csv_chunksize)

    if processes is None:

        # Single-threaded run
        for table in tables:
            _copy_worker(table, engine_args, engine_kwargs, maintenance_work_mem)

    elif type(processes) is int:

        args = zip(
            tables,
            [engine_args] * len(tables),
            [engine_kwargs] * len(tables),
            [maintenance_work_mem] * len(tables)
        )

        try:
            p = Pool(processes)
            result = p.starmap_async(_copy_worker, args, chunksize=1)

        finally:
            del tables
            del hdf
            p.close()
            p.join()

        if not result.successful():
            # If there's an exception, throw it, but we don't care about the
            # results
            result.get()

    else:
        raise ValueError("processes should be int or None.")
