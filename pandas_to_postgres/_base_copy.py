from .utilities import get_logger
from sqlalchemy.schema import AddConstraint, DropConstraint, PrimaryKeyConstraint
from sqlalchemy import MetaData
from sqlalchemy import text
from sqlalchemy import Integer, VARCHAR


# from sqlalchemy.orm import alter_table
from sqlalchemy.exc import SQLAlchemyError


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
        csv_chunksize=10**6,
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
            self.instantiate_attrs(conn, table_obj)
        else:
            self.sql_table = sql_table

    def instantiate_attrs(self, conn, table_obj):
        """
        When using multiprocessing, pickling of logger and SQLAlchemy objects in
        __init__ causes issues, so allow for deferring until after the pickling to fetch
        SQLAlchemy objs

        Parameters
        ----------
        conn: SQLAlchemy Connection
            Managed outside of the object
        table_obj: SQLAlchemy Table
            Model object for the destination SQL Table
        """
        self.conn = conn
        self.table_obj = table_obj
        self.schema = table_obj.schema
        if self.schema:
            self.sql_table = "{}.{}".format(table_obj.schema, table_obj.name)
        else:
            self.sql_table = table_obj.name
        self.logger = get_logger(self.sql_table)
        # args = {"primary_key": ["classification.city_id"]}
        self.primary_key = table_obj.primary_key
        self.foreign_keys = table_obj.foreign_key_constraints

    def drop_pk(self):
        """
        Drop primary key constraints on PostgreSQL table as well as CASCADE any other
        constraints that may rely on the PK
        """
        self.logger.info("Dropping {} primary key".format(self.sql_table))
        self.logger.info("Name of table {}".format(self.sql_table))
        try:
            for pkey in self.primary_key:
                print("primary key", pkey)
            with self.conn.begin_nested():
                # constraints = [self.conn for self.conn in self.table_obj.constraints]
                # for constraint in self.table_obj.constraints:
                #     if isinstance(self.conn, PrimaryKeyConstraint):
                #         print("primary key", constraint)
                #         constraint.drop()
                # print(
                #     "Primary key constraint for table [%s] on: %s"
                #     % (self.sql_table, primary_key_constraints.columns.keys())
                # )

                # print("foreign keys", self.foreign_keys)
                # print("before drop", self.primary_key)
                # self.table_obj.alter_column(
                #     self.sql_table,
                #     self.primary_key,
                #     existing_type=Integer,
                #     type_=VARCHAR(length=25),
                # )
                # self.conn.execute(
                #     "ALTER TABLE {} DROP CONSTRAINT city_cluster_year_pkey".format(
                #         self.sql_table
                #     )
                # )

                self.conn.execute(
                    # text(DropConstraint(self.primary_key, "primary", cascade="all"))
                    DropConstraint(self.primary_key, cascade="all")
                )

                print("after drop", self.primary_key)
        except SQLAlchemyError as e:
            print("ERROR MESSAGE", e)
            self.logger.info(
                "{} primary key not found. Skipping".format(self.sql_table)
            )

    def create_pk(self):
        """Create primary key constraints on PostgreSQL table"""
        self.logger.info("Creating {} primary key".format(self.sql_table))
        try:
            self.conn.execute(AddConstraint(self.primary_key))
        except:
            print("skipping adding pk")

    def drop_fks(self):
        """Drop foreign key constraints on PostgreSQL table"""
        for fk in self.foreign_keys:
            self.logger.info("Dropping foreign key {}".format(fk.name))
            try:
                with self.conn.begin_nested():
                    print("before FK drop", self.foreign_keys)
                    self.conn.execute(DropConstraint(fk))
                    print("after FK drop", self.foreign_keys)
            except SQLAlchemyError:
                self.logger.warn("Foreign key {} not found".format(fk.name))

    def create_fks(self):
        """Create foreign key constraints on PostgreSQL table"""
        for fk in self.foreign_keys:
            try:
                self.logger.info("Creating foreign key {}".format(fk.name))
                self.conn.execute(AddConstraint(fk))
            except SQLAlchemyError:
                self.logger.warn("Error creating foreign key {}".format(fk.name))

    def truncate(self):
        """TRUNCATE PostgreSQL table"""
        self.logger.info("Truncating {}".format(self.sql_table))
        self.conn.execute("TRUNCATE TABLE {} CASCADE;".format(self.sql_table))

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
