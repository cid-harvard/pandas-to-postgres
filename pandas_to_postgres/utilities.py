import logging
import pandas as pd
from sqlalchemy.sql.schema import Table

from collections import defaultdict
from io import StringIO


logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s %(asctime)s.%(msecs)03d %(message)s",
    datefmt="%Y-%m-%d,%H:%M:%S",
)

logger = logging.getLogger("pandas_to_postgres")


class HDFMetadata(object):
    def __init__(self, file_name="./data.h5", keys=None, chunksize=10 ** 7):
        self.file_name = file_name
        self.chunksize = chunksize
        self.sql_to_hdf = defaultdict(set)
        self.levels = {}

        with pd.HDFStore(self.file_name, mode="r") as store:
            self.keys = keys or store.keys()

            for key in self.keys:
                try:
                    metadata = store.get_storer(key).attrs.atlas_metadata
                    logger.info(f"Metadata: {metadata}")
                except AttributeError:
                    logger.info(f"Attribute Error: Skipping {key}")
                    continue

                self.levels[key] = metadata["levels"]

                sql_table = metadata.get("sql_table_name")
                if sql_table:
                    self.sql_to_hdf[sql_table].add(key)
                else:
                    logger.warn(f"No SQL table name found for {key}")


def create_file_object(df):
    """
    Writes pandas dataframe to an in-memory StringIO file object. Adapted from
    https://gist.github.com/mangecoeur/1fbd63d4758c2ba0c470#gistcomment-2086007
    """
    file_object = StringIO()
    df.to_csv(file_object, index=False)
    file_object.seek(0)
    return file_object


def df_generator(df, chunksize):
    """
    Create a generator to iterate over chunks of a dataframe

    Parameters
    ----------
    df: pandas dataframe
        dataframe to iterate over
    chunksize: int
        max number of rows to return in a chunk
    """
    rows = 0
    if not df.shape[0] % chunksize:
        n_chunks = max(df.shape[0] // chunksize, 1)
    else:
        n_chunks = (df.shape[0] // chunksize) + 1

    for i in range(n_chunks):
        logger.info(f"Chunk {i + 1}/{n_chunks}")
        yield df.iloc[rows : rows + chunksize]
        rows += chunksize


def cast_pandas(df, sql_table):
    """
    Pandas does not handle null values in integer or boolean fields out of the
    box, so cast fields that should be these types in the database to object
    fields and change np.nan to None

    Parameters
    ----------
    df: pandas dataframe
        data frame with fields that are desired to be int or bool as float with
        np.nan that should correspond to None

    sql_table: SQLAlchemy model
        destination table object with field names corresponding to those in df

    Returns
    -------
    df: pandas dataframe
        dataframe with fields that correspond to Postgres int, bigint, and bool
        fields changed to objects with None values for null
    """

    for col in sql_table.columns:
        if str(col.type) in ["INTEGER", "BIGINT"]:
            df[col.name] = df[col.name].apply(
                lambda x: None if pd.isna(x) else int(x), convert_dtype=False
            )
        elif str(col.type) == "BOOLEAN":
            df[col.name] = df[col.name].apply(
                lambda x: None if pd.isna(x) else bool(x), convert_dtype=False
            )

    return df


def add_level_metadata(df, hdf_levels):
    """
    Updates dataframe fields for constant "_level" fields

    Parameters
    ----------
    df: pandas DataFrame
    hdf_levels: dict
        dict of level:value fields that are constant for the entire dataframe

    Returns
    ------
    df: pandas DataFrame
    """

    if hdf_levels:
        logger.info("Adding level metadata values")
        for entity, level_value in hdf_levels.items():
            df[entity + "_level"] = level_value

    return df


def classification_to_pandas(
    df,
    optional_fields=[
        "name_es",
        "name_short_en",
        "name_short_es",
        "description_en",
        "description_es",
        "is_trusted",
        "in_rankings",
    ],
):
    """Convert a classification from the format it comes in the classification
    file (which is the format from the 'classifications' github repository)
    into the format that the flask apps use. Mostly just a thing for dropping
    unneeded columns and renaming existing ones.

    The optional_fields allows you to specify which fields should be considered
    optional, i.e. it'll still work if this field doesn't exist in the
    classification, like the description fields for example.
    """

    # Sort fields and change names appropriately
    new_df = df[["index", "code", "name", "level", "parent_id"]]
    new_df = new_df.rename(columns={"index": "id", "name": "name_en"})

    for field in optional_fields:
        if field in df:
            new_df[field] = df[field]

    return new_df
