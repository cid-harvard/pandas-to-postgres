from multiprocessing import Pool
from sqlalchemy import MetaData, create_engine
from .copy_hdf import HDFTableCopy
from .utilities import cast_pandas, get_logger


logger = get_logger("hdf_to_postgres")


def create_hdf_table_objects(
    file_name,
    sql_to_hdf,
    csv_chunksize=10 ** 6,
    hdf_chunksize=10 ** 7,
    hdf_metadata=None,
):
    """
    Create list of HDFTableCopy objects to iterate to run the copy method on each

    Parameters
    ----------
    sql_to_hdf: dict
    csv_chunksize: int
        Maximum number of rows to store in an in-memory StringIO CSV

    Returns
    -------
    tables: list
        List of HDFTableCopy objects
    """
    tables = []

    for sql_table, hdf_tables in sql_to_hdf.items():
        tables.append(
            HDFTableCopy(
                file_name,
                hdf_tables,
                defer_sql_objs=True,
                sql_table=sql_table,
                csv_chunksize=csv_chunksize,
                hdf_chunksize=hdf_chunksize,
                hdf_metadata=hdf_metadata,
            )
        )

    return tables


def copy_worker(
    copy_obj,
    engine_args,
    engine_kwargs,
    maintenance_work_mem=None,
    data_formatters=[cast_pandas],
    data_formatter_kwargs={},
):
    """
    Callable function used in hdf_to_postgres function to execute copy process. Since
    we fork()ed into a new process, the engine contains process specific stuff that
    shouldn't be shared - this creates a fresh Engine with the same settings but without
    those.

    Parameters
    ----------
    copy_obj: HDFTableCopy or subclass
        Object used to execute the copy process
    engine_args: list
        arguments to pass into create_engine()
    engine_kwargs: dict
        keyword arguments to pass into create_engine()
    maintenance_work_mem: str or None
        What to set postgresql's maintenance_work_mem option to: this helps
        when rebuilding large indexes, etc.
    data_formatters: list of func
        Functions to pass to the copy_obj.data_formatting method during copy
    data_formatter_kwargs:
        Kwargs to pass to functions in data_formatters
    """

    engine = create_engine(*engine_args, **engine_kwargs)
    metadata = MetaData(bind=engine)
    metadata.reflect()

    with engine.connect() as conn:

        conn.execution_options(autocommit=True)

        if maintenance_work_mem is not None:
            conn.execute(
                "SET maintenance_work_mem TO '{}';".format(maintenance_work_mem)
            )

        # Get SQLAlchemy Table object
        table_obj = metadata.tables.get(copy_obj.sql_table, None)
        if table_obj is None:
            raise ValueError("Table {} does not exist.".format(copy_obj.sql_table))

        copy_obj.instantiate_sql_objs(conn, table_obj)

        # Run the task
        copy_obj.copy(
            data_formatters=data_formatters, data_formatter_kwargs=data_formatter_kwargs
        )


def hdf_to_postgres(
    file_name,
    engine_args,
    engine_kwargs={},
    keys=[],
    sql_to_hdf=None,
    csv_chunksize=10 ** 6,
    hdf_chunksize=10 ** 7,
    processes=None,
    maintenance_work_mem=None,
    hdf_metadata=None,
):
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
        HDF keys to limit copy to. If falsey, does not restrict.
    sql_to_hdf: dict or None
        Mapping of SQL table names to iterable of corresponding HDF keys
    csv_chunksize: int
        Maximum number of StringIO CSV rows to keep in memory at a time
    hdf_chunksize: int
            Max rows to keep in memory when reading HDF file
    processes: int or None
        If None, run single threaded. If integer, number of processes in the
        multiprocessing Pool
    maintenance_work_mem: str or None
        What to set postgresql's maintenance_work_mem option to: this helps
        when rebuilding large indexes, etc.
    hdf_metadata: dict or None
        Mapping of HDF table keys : dict of constants and values for each table
        e.g., {"cities": {"level": "city"}}
    """
    if keys and sql_to_hdf:
        # Filter HDF tables as union of keys and sql_to_hdf.values()
        filtered_sql_to_hdf = {}
        for sql_table, hdf_tables in sql_to_hdf.items():

            filtered_hdf = set()

            for hdf_table in hdf_tables:
                if hdf_table in keys:
                    filtered_hdf.add(hdf_table)
            if filtered_hdf:
                filtered_sql_to_hdf[sql_table] = filtered_hdf
        sql_to_hdf = filtered_sql_to_hdf
    elif keys and not sql_to_hdf:
        sql_to_hdf = {x: set(x) for x in keys}
    elif not keys and not sql_to_hdf:
        raise ValueError("Keys and sql_to_hdf both undefined")

    tables = create_hdf_table_objects(
        file_name,
        sql_to_hdf,
        csv_chunksize=csv_chunksize,
        hdf_chunksize=hdf_chunksize,
        hdf_metadata=hdf_metadata,
    )

    if processes is None:
        # Single-threaded run
        for table in tables:
            copy_worker(table, engine_args, engine_kwargs, maintenance_work_mem)

    elif type(processes) is int:
        args = zip(
            tables,
            [engine_args] * len(tables),
            [engine_kwargs] * len(tables),
            [maintenance_work_mem] * len(tables),
        )

        try:
            p = Pool(processes)
            result = p.starmap_async(copy_worker, args, chunksize=1)

        finally:
            del tables
            p.close()
            p.join()

        if not result.successful():
            # If there's an exception, throw it, but we don't care about the
            # results
            result.get()

    else:
        raise ValueError("processes should be int or None.")
