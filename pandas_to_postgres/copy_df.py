from .utilities import create_file_object, df_generator, logger, cast_pandas
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
        self.rows = self.df.shape[0]

    def copy(self, functions=[cast_pandas]):
        self.drop_fks()
        self.drop_pk()
        self.df = self.data_formatting(self.df, functions=functions)
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
