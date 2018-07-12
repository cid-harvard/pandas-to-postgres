from .utilities import (
    create_file_object,
    df_generator,
    logger,
    cast_pandas,
    add_level_metadata,
)

import pandas as pd
from sqlalchemy.sql.schema import Table
from sqlalchemy.engine.base import Connection
from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError


class DataFrameCopy(object):
    def __init__(
        self,
        conn: Connection,
        table_obj: Table,
        df: pd.DataFrame,
        levels: dict = None,
        csv_chunksize: int = 10 ** 6,
    ):
        self.conn = conn
        self.table_obj = table_obj
        self.sql_table = self.table_obj.name
        self.df = df
        self.levels = levels
        self.columns = self.df.columns
        self.rows = self.df.shape[0]
        self.csv_chunksize = csv_chunksize
        self.primary_key = self.table_obj.primary_key
        self.foreign_keys = self.table_obj.foreign_key_constraints

    def close_conn(self):
        self.conn.close()
        del self.conn

    def drop_pk(self):
        logger.info(f"Dropping {self.sql_table} primary key")
        try:
            with self.conn.begin_nested():
                self.conn.execute(DropConstraint(self.primary_key, cascade=True))
        except SQLAlchemyError:
            logger.info(f"{self.sql_table} primary key not found. Skipping")

    def create_pk(self):
        logger.info(f"Creating {self.sql_table} primary key")
        self.conn.execute(AddConstraint(self.primary_key))

    def drop_fks(self):
        for fk in self.foreign_keys:
            logger.info(f"Dropping foreign key {fk.name}")
            try:
                with self.conn.begin_nested():
                    self.conn.execute(DropConstraint(fk))
            except SQLAlchemyError:
                logger.warn(f"Foreign key {fk.name} not found")

    def create_fks(self):
        for fk in self.foreign_keys:
            try:
                logger.info(f"Creating foreign key {fk.name}")
                self.conn.execute(AddConstraint(fk))
            except SQLAlchemyError:
                logger.warn(f"Error creating foreign key {fk.name}")

    def truncate(self):
        logger.info(f"Truncating {self.sql_table}")
        self.conn.execute(f"TRUNCATE TABLE {self.sql_table};")

    def analyze(self):
        logger.info(f"Analyzing {self.sql_table}")
        self.conn.execute(f"ANALYZE {self.sql_table};")

    def copy_from_file(self, file_object):
        cur = self.conn.connection.cursor()
        cols = ", ".join([f"{col}" for col in self.columns])
        sql = f"COPY {self.sql_table} ({cols}) FROM STDIN WITH CSV HEADER FREEZE"
        cur.copy_expert(sql=sql, file=file_object)

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
