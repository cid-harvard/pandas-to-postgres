from .utilities import (
    create_file_object,
    df_generator,
    logger,
    classification_to_pandas,
    cast_pandas,
    add_level_metadata,
    HDFMetadata,
)

from ._base_copy import BaseCopy
from typing import List
import pandas as pd
from sqlalchemy.sql.schema import Table
from sqlalchemy.engine.base import Connection


class HDFTableCopy(BaseCopy):
    def __init__(
        self,
        hdf_tables: List[str],
        hdf_meta: HDFMetadata,
        defer_sql_objs: bool = False,
        conn: Connection = None,
        table_obj: Table = None,
        sql_table: str = None,
        csv_chunksize: int = 10 ** 6,
    ):
        super().__init__(
            self, defer_sql_objs, conn, table_obj, sql_table, csv_chunksize
        )

        self.hdf_tables = hdf_tables

        # Info from the HDFMetadata object
        self.levels = hdf_meta.levels
        self.file_name = hdf_meta.file_name
        self.hdf_chunksize = hdf_meta.chunksize

    def copy_table(self):
        self.drop_fks()
        self.drop_pk()

        # These need to be one transaction to use COPY FREEZE
        with self.conn.begin():
            self.truncate()
            self.hdf_to_pg()

        self.create_pk()
        self.create_fks()
        self.analyze()

    def hdf_to_pg(self):
        if self.hdf_tables is None:
            logger.warn(f"No HDF table found for SQL table {self.sql_table}")
            return

        for hdf_table in self.hdf_tables:
            logger.info(f"*** {hdf_table} ***")
            hdf_levels = self.levels.get(hdf_table)

            logger.info("Reading HDF table")
            df = pd.read_hdf(self.file_name, key=hdf_table)
            self.rows += len(df)

            # Handle NaN --> None type casting and adding const level data
            df = cast_pandas(df, self.table_obj)
            df = add_level_metadata(df, hdf_levels)

            if self.columns is None:
                self.columns = df.columns

            logger.info("Creating generator for chunking dataframe")
            for chunk in df_generator(df, self.csv_chunksize):

                logger.info("Creating CSV in memory")
                fo = create_file_object(chunk)

                logger.info("Copying chunk to database")
                self.copy_from_file(fo)
                del fo
            del df
        logger.info(f"All chunks copied ({self.rows} rows)")


class ClassificationHDFTableCopy(HDFTableCopy):
    def __init__(
        self,
        hdf_tables: List[str],
        hdf_meta: HDFMetadata,
        defer_sql_objs: bool = False,
        conn: Connection = None,
        table_obj: Table = None,
        sql_table: str = None,
        csv_chunksize: int = 10 ** 6,
    ):
        super().__init__(
            self,
            hdf_tables,
            hdf_meta,
            defer_sql_objs,
            conn,
            table_obj,
            sql_table,
            csv_chunksize,
        )

    def hdf_to_pg(self):
        if self.hdf_tables is None:
            logger.warn("No HDF table found for SQL table {self.sql_table}")
            return

        for hdf_table in self.hdf_tables:
            logger.info(f"*** {hdf_table} ***")
            logger.info("Reading HDF table")
            df = pd.read_hdf(self.file_name, key=hdf_table)
            self.rows += len(df)

            logger.info("Formatting classification")
            df = classification_to_pandas(df)
            df = cast_pandas(df, self.table_obj)

            if self.columns is None:
                self.columns = df.columns

            logger.info("Creating CSV in memory")
            fo = create_file_object(df)

            logger.info("Copying table to database")
            self.copy_from_file(fo)
            del df
            del fo
        logger.info(f"All chunks copied ({self.rows} rows)")


class BigHDFTableCopy(HDFTableCopy):
    def __init__(
        self,
        hdf_tables: List[str],
        hdf_meta: HDFMetadata,
        defer_sql_objs: bool = False,
        conn: Connection = None,
        table_obj: Table = None,
        sql_table: str = None,
        csv_chunksize: int = 10 ** 6,
    ):
        super().__init__(
            self,
            hdf_tables,
            hdf_meta,
            defer_sql_objs,
            conn,
            table_obj,
            sql_table,
            csv_chunksize,
        )

    def hdf_to_pg(self):
        if self.hdf_tables is None:
            logger.warn(f"No HDF table found for SQL table {self.sql_table}")
            return

        for hdf_table in self.hdf_tables:
            logger.info(f"*** {hdf_table} ***")
            hdf_levels = self.levels.get(hdf_table)

            with pd.HDFStore(self.file_name) as store:
                nrows = store.get_storer(hdf_table).nrows

            self.rows += nrows
            if nrows % self.hdf_chunksize:
                n_chunks = (nrows // self.hdf_chunksize) + 1
            else:
                n_chunks = nrows // self.hdf_chunksize

            start = 0

            for i in range(n_chunks):
                logger.info(f"*** HDF chunk {i + 1} of {n_chunks} ***")
                logger.info("Reading HDF table")
                stop = min(start + self.hdf_chunksize, nrows)
                df = pd.read_hdf(self.file_name, key=hdf_table, start=start, stop=stop)

                start += self.hdf_chunksize

                # Handle NaN --> None type casting and adding const level data
                df = cast_pandas(df, self.table_obj)
                df = add_level_metadata(df, hdf_levels)

                if self.columns is None:
                    self.columns = df.columns

                logger.info("Creating generator for chunking dataframe")
                for chunk in df_generator(df, self.csv_chunksize):
                    logger.info("Creating CSV in memory")
                    fo = create_file_object(chunk)

                    logger.info("Copying chunk to database")
                    self.copy_from_file(fo)
                    del fo
                del df
        logger.info(f"All chunks copied ({self.rows} rows)")
