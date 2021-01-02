from .utilities import create_file_object, df_generator, cast_pandas
from ._base_copy import BaseCopy


class DataFrameCopyAppend(BaseCopy):
    """
    Differs from main DataFrameCopy class with that it doesn't drop fks, pks and indexes, and appends df
    to existing data in table. This will cause it to lose a lot of performance.
    """

    def __init__(
            self, df, defer_sql_objs=False, conn=None, table_obj=None, csv_chunksize=10 ** 6
    ):
        """
        Parameters
        ----------
        df: pandas DataFrame
            Data to copy to database table
        defer_sql_objs: bool
            multiprocessing has issue with passing SQLALchemy objects, so if
            True, defer attributing these to the object until after pickled by Pool
        conn: SQlAlchemy Connection
            Managed outside of the object
        table_obj: SQLAlchemy model object
            Destination SQL Table
        csv_chunksize: int
            Max rows to keep in memory when generating CSV for COPY
        """
        super().__init__(defer_sql_objs, conn, table_obj, csv_chunksize)

        self.df = df
        self.rows = self.df.shape[0]

    def truncate(self):
        pass

    def create_pk(self):
        pass

    def create_fks(self):
        pass

    def drop_fks(self):
        pass

    def drop_pk(self):
        pass

    def copy_from_file(self, file_object):
        """
        COPY to PostgreSQL table using StringIO CSV object
        Parameters
        ----------
        file_object: StringIO
            CSV formatted data to COPY from DataFrame to PostgreSQL
        """
        cur = self.conn.connection.cursor()
        file_object.seek(0)
        columns = file_object.readline()
        sql = "COPY {table} ({columns}) FROM STDIN WITH  CSV".format(
            table=self.sql_table, columns=columns
        )
        cur.copy_expert(sql=sql, file=file_object)

    def copy(self, functions=[cast_pandas]):
        self.drop_fks()
        self.drop_pk()
        self.df = self.data_formatting(self.df, functions=functions)
        with self.conn.begin():
            self.truncate()

            self.logger.info("Creating generator for chunking dataframe")
            for chunk in df_generator(self.df, self.csv_chunksize):
                self.logger.info("Creating CSV in memory")
                fo = create_file_object(chunk)

                self.logger.info("Copying chunk to database")
                self.copy_from_file(fo)
                del fo

            self.logger.info("All chunks copied ({} rows)".format(self.rows))

        self.create_pk()
        self.create_fks()
        self.analyze()
