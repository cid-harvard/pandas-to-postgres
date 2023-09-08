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
        # self.foreign_keys_name = table_obj.foreign_keys

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
                self.conn.execute(DropConstraint(self.primary_key, cascade="all"))
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
        print("FOREIGN KEYS SET", self.foreign_keys)
        for fk in self.foreign_keys:
            # print("FOREIGN KEY ", fk)
            self.logger.info("Dropping foreign key {}".format(fk.name))
            try:
                with self.conn.begin_nested():
                    # print("before FK drop", self.foreign_keys)
                    self.conn.execute(DropConstraint(fk), cascade="all")
                    # print("after FK drop", self.foreign_keys)
            except SQLAlchemyError:
                self.logger.warn("Foreign key {} not found".format(fk.name))

    def create_fks(self):
        """Create foreign key constraints on PostgreSQL table"""
        print("TABLE NAME IN CREATE FOREIGN KEYS", self.sql_table)
        if self.sql_table == "naics_industry":
            print("***!!*** trying to add foreign key")
            print("BEFORE FOREIGN KEY ADDED", self.foreign_keys)
            # self.foreign_keys.elements[0]
            try:
                self.conn.execute(
                    text(
                        "ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {}({})".format(
                            self.sql_table,
                            "naics_id",
                            "classification.naics_industry",
                            "naics_id",
                        )
                    )
                )
                print("ADDED FOREIGN KEY", self.foreign_keys)
            except SQLAlchemyError:
                self.logger.warn("Error creating foreign key {}".format(fk.name))
        for fk in self.foreign_keys:
            try:
                self.logger.info("Creating foreign key {}".format(fk.name))
                # self.conn.execute(AddConstraint(fk.elements[0]))
                # self.conn.execute(AddConstraint(fk))
                self.conn.execute(
                    text(
                        "ALTER TABLE {} CREATE CONSTRAINT {}".format(
                            self.sql_table, fk.name
                        )
                    )
                )
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
