import logging
from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s %(message)s",
    datefmt="%Y-%m-%d,%H:%M:%S",
)


class BaseCopy(object):
    """
    Parent class for all common attibutes and methods for copy objects
    """

    def __init__(
        self,
        defer_sql_objs=False,
        conn=None,
        table_obj=None,
        sql_table=None,
        csv_chunksize=10 ** 6,
    ):
        """
        Parameters
        ----------
        defer_sql_objs: bool
            multiprocessing has issue with passing SQLALchemy objects, so if
            True, defer attributing these to the object until after pickled by Pool
        conn: SQLAlchemy Connection
            Managed outside of the object
        table_obj: SQLAlchemy Table
            Model object for the destination SQL Table
        sql_table: string
            SQL table name
        csv_chunksize: int
            Max rows to keep in memory when generating CSV for COPY
        """

        self.rows = 0
        self.csv_chunksize = csv_chunksize

        if not defer_sql_objs:
            self.instantiate_sql_objs(conn, table_obj)
        else:
            self.sql_table = sql_table

        self.logger = logging.getLogger(self.sql_table)

    def instantiate_sql_objs(self, conn, table_obj):
        """
        When using multiprocessing, pickling of SQLAlchemy objects in __init__ causes
        issues, so allow for deferring until after the pickling to fetch SQLAlchemy objs

        Parameters
        ----------
        conn: SQLAlchemy Connection
            Managed outside of the object
        table_obj: SQLAlchemy Table
            Model object for the destination SQL Table
        """
        self.conn = conn
        self.table_obj = table_obj
        self.sql_table = table_obj.name
        self.primary_key = table_obj.primary_key
        self.foreign_keys = table_obj.foreign_key_constraints

    def drop_pk(self):
        """
        Drop primary key constraints on PostgreSQL table as well as CASCADE any other
        constraints that may rely on the PK
        """
        self.logger.info("Dropping {} primary key".format(self.sql_table))
        try:
            with self.conn.begin_nested():
                self.conn.execute(DropConstraint(self.primary_key, cascade=True))
        except SQLAlchemyError:
            self.logger.info(
                "{} primary key not found. Skipping".format(self.sql_table)
            )

    def create_pk(self):
        """Create primary key constraints on PostgreSQL table"""
        self.logger.info("Creating {} primary key".format(self.sql_table))
        self.conn.execute(AddConstraint(self.primary_key))

    def drop_fks(self):
        """Drop foreign key constraints on PostgreSQL table"""
        for fk in self.foreign_keys:
            self.logger.info("Dropping foreign key {}".format(fk.name))
            try:
                with self.conn.begin_nested():
                    self.conn.execute(DropConstraint(fk))
            except SQLAlchemyError:
                self.logger.warn("Foreign key {} not found".format(fk.name))

    def create_fks(self):
        """Create foreign key constraints on PostgreSQL table"""
        for fk in self.foreign_keys:
            try:
                logger.info("Creating foreign key {}".format(fk.name))
                self.conn.execute(AddConstraint(fk))
            except SQLAlchemyError:
                logger.warn("Error creating foreign key {}".format(fk.name))

    def truncate(self):
        """TRUNCATE PostgreSQL table"""
        self.logger.info("Truncating {}".format(self.sql_table))
        self.conn.execute("TRUNCATE TABLE {};".format(self.sql_table))

    def analyze(self):
        """Run ANALYZE on PostgreSQL table"""
        self.logger.info("Analyzing {}".format(self.sql_table))
        self.conn.execute("ANALYZE {};".format(self.sql_table))

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
        sql = "COPY {table} ({columns}) FROM STDIN WITH CSV FREEZE".format(
            table=self.sql_table, columns=columns
        )
        cur.copy_expert(sql=sql, file=file_object)

    def data_formatting(self, df, functions=[], **kwargs):
        """
        Call each function in the functions list arg on the DataFrame and return

        Parameters
        ----------
        df: pandas DataFrame
            DataFrame to format
        functions: list of functions
            Functions to apply to df. each gets passed df, self as copy_obj, and all
            kwargs passed to data_formatting
        **kwargs
            kwargs to pass on to each function

        Returns
        -------
        df: pandas DataFrame
            formatted DataFrame
        """
        for f in functions:
            df = f(df, copy_obj=self, **kwargs)
        return df
