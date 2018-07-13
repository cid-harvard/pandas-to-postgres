from .utilities import (
    create_file_object,
    df_generator,
    logger,
    cast_pandas,
    add_level_metadata,
)

from ._base_copy import BaseCopy

import pandas as pd
from sqlalchemy.sql.schema import Table
from sqlalchemy.engine.base import Connection


class DataFrameCopy(BaseCopy):
    def __init__(
        self,
        df: pd.DataFrame,
        defer_sql_objs: bool = False,
        conn: Connection = None,
        table_obj: Table = None,
        csv_chunksize: int = 10 ** 6,
        levels: dict = None,
    ):
        super().__init__(defer_sql_objs, conn, table_obj, csv_chunksize)

        self.df = df
        self.levels = levels
        self.columns = self.df.columns
        self.rows = self.df.shape[0]

    def format_df(self):
        # Handle NaN --> None type casting
        self.df = cast_pandas(self.df, self.table_obj)

        # Add level (constant) data to frames from dict
        if self.levels:
            self.df = add_level_metadata(self.df, self.levels)

    def copy(self):
        self.drop_fks()
        self.drop_pk()
        self.format_df()
        with self.conn.begin():
            self.truncate()

            logger.info("Creating generator for chunking dataframe")
            for chunk in df_generator(self.df, self.csv_chunksize):

                logger.info("Creating CSV in memory")
                fo = create_file_object(chunk)

                logger.info("Copying chunk to database")
                self.copy_from_file(fo)
                del fo

            logger.info(f"All chunks copied ({self.rows} rows)")

        self.create_pk()
        self.create_fks()
        self.analyze()
