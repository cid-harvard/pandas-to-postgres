import logging
from collections import defaultdict
from pandas import isna, HDFStore
from io import StringIO


def get_logger(name):
    print("TESTING!!!!")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s %(message)s",
        datefmt="%Y-%m-%d,%H:%M:%S",
    )

    return logging.getLogger(name)


def hdf_metadata(file_name, keys=None, metadata_attr=None, metadata_keys=[]):
    """
    Returns metadata stored in the HDF file including a mapping of SQL table names to
    corresponding HDF tables (assuming one:many) as well as a dictionary of keys
    corresponding to HDF tables with a dictionary of constants as values.

    Parameters
    ----------
    file_name: str
        path to hdf file to copy from
    keys: list of strings
        HDF keys to copy data from
    metadata_attr: str
        Location of relevant metadata in store.get_storer().attrs
    metadata_keys: list of strings
        Keys to get from metadata store

    Returns
    -------
    sql_to_hdf: dict of str:set
        Mapping of SQL tables :  set of HDF table keys
        e.g., {"locations": {"countries", "states", "cities"}}
    metadata_vars: dict of str:dict
        Mapping of HDF table keys : dict of constants and values for each table
        e.g., {"cities": {"level": "city"}}
    """

    sql_to_hdf = defaultdict(set)
    metadata_vars = defaultdict(dict)
    logger = get_logger("hdf_metadata")

    with HDFStore(file_name, mode="r") as store:
        keys = keys or store.keys()

        if metadata_attr:
            for key in keys:
                try:
                    metadata = store.get_storer(key).attrs[metadata_attr]
                    logger.info("Metadata: {}".format(metadata))
                except (AttributeError, KeyError):
                    if "/meta" not in key:
                        logger.info(
                            "No metadata found for key '{}'. Skipping".format(key)
                        )
                    continue

                for mkey in metadata_keys:
                    metadata_vars[mkey][key] = metadata.get(mkey)

                sql_table = metadata.get("sql_table_name")

                if sql_table:
                    sql_to_hdf[sql_table].add(key)
                else:
                    logger.warn("No SQL table name found for {}".format(key))

    return sql_to_hdf, metadata_vars


def create_file_object(df):
    """
    Writes pandas dataframe to an in-memory StringIO file object. Adapted from
    https://gist.github.com/mangecoeur/1fbd63d4758c2ba0c470#gistcomment-2086007

    Parameters
    ----------
    df: pandas DataFrame

    Returns
    -------
    file_object: StringIO
    """
    file_object = StringIO()
    df.to_csv(file_object, index=False)
    file_object.seek(0)
    return file_object


def df_generator(df, chunksize=10**6, logger=None):
    """
    Create a generator to iterate over chunks of a dataframe

    Parameters
    ----------
    df: pandas dataframe
        Data to iterate over
    chunksize: int
        Max number of rows to return in a chunk
    """
    rows = 0
    if not df.shape[0] % chunksize:
        n_chunks = max(df.shape[0] // chunksize, 1)
    else:
        n_chunks = (df.shape[0] // chunksize) + 1

    for i in range(n_chunks):
        if logger:
            logger.info("Chunk {i}/{n}".format(i=i + 1, n=n_chunks))
        yield df.iloc[rows : rows + chunksize]
        rows += chunksize


def cast_pandas(df, columns=None, copy_obj=None, logger=None, **kwargs):
    """
    Pandas does not handle null values in integer or boolean fields out of the
    box, so cast fields that should be these types in the database to object
    fields and change np.nan to None

    Parameters
    ----------
    df: pandas DataFrame
        data frame with fields that are desired to be int or bool as float with
        np.nan that should correspond to None
    columns: list of SQLAlchemy Columns
        Columnsto iterate through to determine data types
    copy_obj: BaseCopy or subclass
        instance of BaseCopy passed from the BaseCopy.data_formatting method where
        we can access BaseCopy.table_obj.columns

    Returns
    -------
    df: pandas DataFrame
        DataFrame with fields that correspond to Postgres int, bigint, and bool
        fields changed to objects with None values for null
    """

    logger = get_logger("cast_pandas")

    if columns is None and copy_obj is None:
        raise ValueError("One of columns or copy_obj must be supplied")

    columns = columns or copy_obj.table_obj.columns
    for col in columns:
        try:
            if str(col.type) in ["INTEGER", "BIGINT"]:
                df[col.name] = df[col.name].apply(
                    lambda x: None if isna(x) else int(x), convert_dtype=False
                )
            elif str(col.type) == "BOOLEAN":
                df[col.name] = df[col.name].apply(
                    lambda x: None if isna(x) else bool(x), convert_dtype=False
                )
        except KeyError:
            logger.warn(
                "Column {} not in DataFrame. Cannot coerce object type.".format(
                    col.name
                )
            )

    return df
