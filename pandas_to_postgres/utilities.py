from pandas import isna
from io import StringIO


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


def df_generator(df, chunksize=10 ** 6, logger=None):
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


def cast_pandas(df, columns=None, copy_obj=None, **kwargs):
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

    if columns is None and copy_obj is None:
        raise ValueError("One of columns or copy_obj must be supplied")

    columns = columns or copy_obj.table_obj.columns
    for col in columns:
        if str(col.type) in ["INTEGER", "BIGINT"]:
            df[col.name] = df[col.name].apply(
                lambda x: None if isna(x) else int(x), convert_dtype=False
            )
        elif str(col.type) == "BOOLEAN":
            df[col.name] = df[col.name].apply(
                lambda x: None if isna(x) else bool(x), convert_dtype=False
            )

    return df
