from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from datetime import datetime
from pytz import timezone
import time

from typing import TYPE_CHECKING, Optional, Sequence, Iterable, Mapping, Any

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MySqlToPostgresOperator(BaseOperator):
    """
    Saves data from a specific SQL query DB source into DB target.

    :param query: the sql query to be executed.
    :param mysql_conn: reference to a specific connection of database source.
    :param target_table: postgres table into where the data will be sent.
    :param identifier: column of unique key to be identifier
    :param postgres_conn: reference to a specific connection of database target.
    :param db_query_from: reference to which db to execute query (source or target).
        You can choose either `mysql` or `postgres`.
    """

    template_fields: Sequence[str] = (
        'query',
    )
    template_ext: Sequence[str] = ('.sql', '.json')
    template_fields_renderers = {
        "query": "sql",
    }

    def __init__(
            self,
            *,
            query: str = None,
            target_table: str = None,
            identifier: [str] = None,
            mysql_conn: str = None,
            postgres_conn: str = None,
            replace: Optional[bool] = False,
            db_query_from: str = 'mysql',
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.target_table = target_table
        self.identifier = identifier
        self.mysql_conn = mysql_conn
        self.postgres_conn = postgres_conn
        self.replace = replace,
        self.db_query_from = db_query_from

        # params that will be passed
        self.row_count = 0
        self.current_time = datetime.now(timezone('Asia/Jakarta'))
        self.duration = 0

    @staticmethod
    def _serialize_cell(cell) -> str:
        """
        Returns the SQL literal of the cell as a string.

        :param cell: The cell to insert into the table
        :return: The serialized cell
        """
        if cell is None:
            return "NULL"
        if isinstance(cell, datetime):
            return "'" + cell.isoformat() + "'"
        if isinstance(cell, str):
            return "'" + str(cell).replace("'", "''") + "'"
        return "'" + str(cell) + "'"

    @staticmethod
    def get_column(target_fields) -> str:
        if target_fields:
            target_fields_fragment = ", ".join(target_fields)
            target_fields_fragment = f"({target_fields_fragment})"
        else:
            target_fields_fragment = ""

        return target_fields_fragment

    def get_value(self, rows) -> str:
        values = ""
        for i, row in enumerate(rows, 1):
            lst = []
            for cell in row:
                cell = cell.replace('\0', '') if isinstance(cell, str) else cell
                lst.append(self._serialize_cell(cell))
            lsj = '(' + ','.join(lst).replace("None", "") + ')'
            values += "" + lsj + ","
        values = values[:-1]
        return values

    def generate_query(self, rows, target_fields) -> str:

        sql = f"""
            INSERT INTO {self.target_table}
            {self.get_column(target_fields)}
            VALUES {self.get_value(rows)}
        """

        if str(self.replace) == "(True,)":
            if target_fields is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names")
            if self.identifier is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires an unique index")
            if isinstance(self.identifier, list):
                replace_index = ",".join(self.identifier)
            else:
                replace_index = self.identifier

            replace_target = [
                "{0} = excluded.{0}".format(col) for col in target_fields if col not in self.identifier
            ]

            sql += f"ON CONFLICT ({replace_index}) DO UPDATE SET {', '.join(replace_target)}"

        return sql

    def execute(self, context: 'Context') -> None:
        self.current_time = datetime.now(timezone('Asia/Jakarta'))
        dateStart = (time.time() * 1000)
        source = MySqlHook(self.mysql_conn)
        target = PostgresHook(self.postgres_conn, log_sql=False)

        # Check if mysql-to-postgres(raw) or postgres-to-postgres(mart)
        if self.db_query_from == 'postgres':
            conn = target.get_conn()
        else:
            conn = source.get_conn()
        cursor = conn.cursor()

        self.log.info(self.query)
        # Execute query
        cursor.execute(self.query)

        # Estimate row count
        self.row_count = cursor.rowcount

        # Find column name based on query result
        target_fields = [x[0] for x in cursor.description]

        # Store query result to rows
        rows = cursor.fetchall()

        # Perform inserting data
        if (rows):
            target.run(
                sql=self.generate_query(rows, target_fields)
            )
        else:
            self.log.info("There is no data to insert/update.")

        # target.insert_rows(
        #     table=self.target_table,
        #     rows=rows,
        #     target_fields=target_fields,
        #     replace_index=self.identifier,
        #     replace=self.replace
        # )

        cursor.close()
        conn.close()

        self.duration = (time.time() * 1000) - dateStart


class MySqlToPostgresOperatorWithReturnValue(MySqlToPostgresOperator):
    """
    MysqlToPostgresOperator with return value that need to be inserted to history table
    """

    template_fields: Sequence[str] = (
        'query',
    )
    template_ext: Sequence[str] = ('.sql', '.json')
    template_fields_renderers = {
        "query": "sql",
    }

    def __init__(
            self,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)

    def post_execute(self, context: Any, result: Any = None):
        self.log.info("Value to be pushed to XComm..")
        self.log.info(f"num row : {self.row_count}")

        ti = context["ti"]
        ti.xcom_push("num_row", self.row_count)
        ti.xcom_push("actdate", str(self.current_time))
        self.log.info(f"actdate : {self.current_time}")

        ti.xcom_push("duration", self.duration)
        self.log.info(f"duration : {self.duration}")
