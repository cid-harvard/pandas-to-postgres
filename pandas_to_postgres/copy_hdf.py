from .utilities import (
    create_file_object,
    df_generator,
    logger,
    classification_to_pandas,
    cast_pandas,
    add_level_metadata,
)

import pandas as pd
from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError


class HDFTableCopy(object):

    rows = 0
    columns = None

    def __init__(self, sql_table, hdf_tables, hdf_meta, csv_chunksize=10 ** 6):
        self.sql_table = sql_table
        self.hdf_tables = hdf_tables
        self.csv_chunksize = csv_chunksize

        # Info from the HDFMetadata object
        self.levels = hdf_meta.levels
        self.file_name = hdf_meta.file_name
        self.hdf_chunksize = hdf_meta.chunksize

    def table_metadata(self):
        self.table_obj = db.metadata.tables[self.sql_table]
        self.primary_key = self.table_obj.primary_key
        self.foreign_keys = self.table_obj.foreign_key_constraints

    def set_conn(self, conn):
        self.conn = conn

    def delete_conn(self):
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

    def copy_table(self):
        self.table_metadata()
        self.drop_fks()
        self.drop_pk()
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
    def __init__(self, sql_table, hdf_tables, hdf_meta, csv_chunksize=10 ** 6):
        HDFTableCopy.__init__(self, sql_table, hdf_tables, hdf_meta, csv_chunksize)

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
    def __init__(self, sql_table, hdf_tables, hdf_meta, csv_chunksize=10 ** 6):
        HDFTableCopy.__init__(self, sql_table, hdf_tables, hdf_meta, csv_chunksize)

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
