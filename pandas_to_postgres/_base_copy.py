from .utilities import logger
from io import StringIO
from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.schema import Table
from sqlalchemy.engine.base import Connection


class BaseCopy(object):
    """
    Parent class for all common attibutes and methods for copy objects
    """

    def __init__(
        self,
        defer_sql_objs: bool = False,
        conn: Connection = None,
        table_obj: Table = None,
        sql_table: str = None,
        csv_chunksize: int = 10 ** 6,
    ):
        """
        Parameters
        ----------
        defer_sql_objs: multiprocessing has issue with passing SQLALchemy objects, so if
            True, defer attributing these to the object until after pickled by Pool
        conn: SQLAlchemy connection managed outside of the object
        table_obj: SQLAlchemy object for the destination SQL Table
        sql_table: string of SQL table name
        csv_chunksize: max rows to keep in memory when generating CSV for COPY
        """

        self.rows = 0
        self.columns = None
        self.csv_chunksize = csv_chunksize

        if not defer_sql_objs:
            self.instantiate_sql_objs(conn, table_obj)
        else:
            self.sql_table = sql_table

    def instantiate_sql_objs(self, conn, table_obj):
        """
        When using multiprocessing, pickling of SQLAlchemy objects in __init__ causes
        issues, so allow for deferring until after the pickling to fetch SQLAlchemy objs
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
        logger.info(f"Dropping {self.sql_table} primary key")
        try:
            with self.conn.begin_nested():
                self.conn.execute(DropConstraint(self.primary_key, cascade=True))
        except SQLAlchemyError:
            logger.info(f"{self.sql_table} primary key not found. Skipping")

    def create_pk(self):
        """Create primary key constraints on PostgreSQL table"""
        logger.info(f"Creating {self.sql_table} primary key")
        self.conn.execute(AddConstraint(self.primary_key))

    def drop_fks(self):
        """Drop foreign key constraints on PostgreSQL table"""
        for fk in self.foreign_keys:
            logger.info(f"Dropping foreign key {fk.name}")
            try:
                with self.conn.begin_nested():
                    self.conn.execute(DropConstraint(fk))
            except SQLAlchemyError:
                logger.warn(f"Foreign key {fk.name} not found")

    def create_fks(self):
        """Create foreign key constraints on PostgreSQL table"""
        for fk in self.foreign_keys:
            try:
                logger.info(f"Creating foreign key {fk.name}")
                self.conn.execute(AddConstraint(fk))
            except SQLAlchemyError:
                logger.warn(f"Error creating foreign key {fk.name}")

    def truncate(self):
        """TRUNCATE PostgreSQL table"""
        logger.info(f"Truncating {self.sql_table}")
        self.conn.execute(f"TRUNCATE TABLE {self.sql_table};")

    def analyze(self):
        """Run ANALYZE on PostgreSQL table"""
        logger.info(f"Analyzing {self.sql_table}")
        self.conn.execute(f"ANALYZE {self.sql_table};")

    def copy_from_file(self, file_object: StringIO):
        """COPY to PostgreSQL table using StringIO CSV object"""
        cur = self.conn.connection.cursor()
        cols = ", ".join([f"{col}" for col in self.columns])
        sql = f"COPY {self.sql_table} ({cols}) FROM STDIN WITH CSV HEADER FREEZE"
        cur.copy_expert(sql=sql, file=file_object)
