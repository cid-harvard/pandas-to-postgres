import pyarrow.parquet as pq
from .utilities import create_file_object, df_generator, cast_pandas
from ._base_copy import BaseCopy
from typing import Optional


class ParquetCopy(BaseCopy):
    """
    Class for handling a standard case of reading a Parquet file into a Pandas
    DataFrame, iterating over it in chunks, and COPYing to PostgreSQL via StringIO CSV
    """

    def __init__(
        self,
        file_name: str,
        defer_sql_objs: bool = False,
        conn=None,
        table_obj=None,
        sql_table: Optional[str] = None,
        schema: Optional[str] = None,
        csv_chunksize=10**6,
        parquet_chunksize=10**7,
    ):
        super().__init__(defer_sql_objs, conn, table_obj, sql_table, csv_chunksize)

        self.parquet_file = pq.ParquetFile(file_name)
        self.parquet_chunksize = parquet_chunksize
        self.total_rows = self.parquet_file.metadata.num_rows

        self.logger.info("*** {} ***".format(file_name))

        if self.total_rows > self.parquet_chunksize:
            if self.total_rows % self.parquet_chunksize:
                self.total_chunks = (self.total_rows // self.parquet_chunksize) + 1
            else:
                self.total_chunks = self.total_rows // self.parquet_chunksize

            self.big_copy = True
        else:
            self.total_chunks = 1
            self.big_copy = False

    def copy(self, data_formatters=[cast_pandas], data_formatter_kwargs={}):
        """
        Go through sequence to COPY data to PostgreSQL table, including dropping Primary
        and Foreign Keys to optimize speed, TRUNCATE table, COPY data, recreate keys,
        and run ANALYZE.

        Parameters
        ----------
        data_formatters: list of functions to apply to df during sequence. Note that
            each of these functions should be able to handle kwargs for one another
        data_formatter_kwargs: list of kwargs to pass to data_formatters functions
        """
        self.drop_fks()
        self.drop_pk()

        # These need to be one transaction to use COPY FREEZE
        with self.conn():
            self.truncate()
            if self.big_copy:
                self.big_parquet_to_pg(
                    data_formatters=data_formatters,
                    data_formatter_kwargs=data_formatter_kwargs,
                )
            else:
                self.parquet_to_pg(
                    data_formatters=data_formatters,
                    data_formatter_kwargs=data_formatter_kwargs,
                )
            self.conn.commit()

        self.create_pk()
        self.create_fks()
        self.analyze()

    def parquet_to_pg(self, data_formatters=[cast_pandas], data_formatter_kwargs={}):
        self.logger.info("Reading Parquet file")
        df = self.parquet_file.read().to_pandas()

        self.logger.info("Formatting data")
        df = self.data_formatting(
            df, functions=data_formatters, **data_formatter_kwargs
        )

        self.logger.info("Creating generator for chunking dataframe")
        for chunk in df_generator(df, self.csv_chunksize, logger=self.logger):
            self.logger.info("Creating CSV in memory")
            fo = create_file_object(chunk)

            self.logger.info("Copying chunk to database")
            self.copy_from_file(fo)
            del fo
        del df
        self.logger.info(f"All chunks copied ({self.total_rows} rows)")

    def big_parquet_to_pg(
        self, data_formatters=[cast_pandas], data_formatter_kwargs={}
    ):
        copied_rows = 0
        n_chunk = 0
        for batch in self.parquet_file.iter_batches(batch_size=self.parquet_chunksize):
            n_chunk += 1
            self.logger.info(f"*** Parquet chunk {n_chunk} of {self.total_chunks} ***")
            df = batch.to_pandas()
            batch_rows = df.shape[0]

            self.logger.info("Formatting data")
            df = self.data_formatting(
                df, functions=data_formatters, **data_formatter_kwargs
            )

            self.logger.info("Creating generator for chunking dataframe")
            for chunk in df_generator(df, self.csv_chunksize, logger=self.logger):
                self.logger.info("Creating CSV in memory")
                fo = create_file_object(chunk)

                self.logger.info("Copying chunk to database")
                self.copy_from_file(fo)
                del fo
            del df

            copied_rows += batch_rows

            self.logger.info(f"Copied {copied_rows:,} of {self.total_rows:,} rows")

        self.logger.info(f"All chunks copied ({self.total_rows:,} rows)")
