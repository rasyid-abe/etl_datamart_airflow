
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from datetime import datetime
import pandas as pd
import json

from typing import TYPE_CHECKING, Optional, Sequence, Union, cast

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MongoToPostgresOperator(BaseOperator):
    """
    Saves data from a specific query MongoDB Source into Postgresql DB target.

    :param query: the mongo query to be executed.
    :param target_table: postgres table into where the data will be sent.
    :param identifier: column of unique key to be identifier,
        usually primary key / unique key from target table.
    :param postgres_conn: reference to a specific connection of database target.
    :param replace: flagging of method insert into table (upsert or only insert),
        currently this operator only support upsert method.
    :param mongo_conn_id: reference to a specific connection of database source.
    :param mongo_collection: reference to a specific source of collection, collection = table in rdbms.
    :param mongo_db: reference to name of database of db source.
    :param mongo_projection: list of fields, to specify or restrict fields to return. 1 = include, 0 = exclude.
       References: https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/
        ex:
         - {"field_name": value}
         - {"item": 1, "status": 1}
    :param incremental_column: field that used to filter collection,
     usually field that store value represent the most recent change to any of the data stored in the table.
    :param start_increment: reference to minimum value of timestamp used to filter data
    :param end_increment: reference to maximum value of timestamp used to filter data
    :param new_column: reference collection of new-old field name,
        if we need to store result of query in different field name.
        Template : {"old field name": "new field name"}
        ex:
        {"_id":"id"}
    :param timezone: represent of timezone that source use, need to be define because our target DB need time awareness value.
        Usually used if source data didnt store timezone, or their stored timezone didnt represent actual timezone.
    """

    template_fields: Sequence[str] = (
        'query',
        'mongo_query',
        'mongo_collection',
        'start_increment',
        'end_increment'
    )
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {
        "query": "sql",
    }

    def __init__(
            self,
            *,
            query: str = None,
            target_table: str = None,
            identifier: [str] = None,
            postgres_conn: str = None,
            replace: bool = True,
            mongo_conn_id: str = None,
            mongo_collection: Optional[str] = None,
            mongo_db: Optional[str] = None,
            mongo_projection: Optional[Union[list, dict]] = None,
            incremental_column: list = None,
            start_increment: str,
            end_increment: str,
            new_column: Optional[dict] = None,
            timezone: Optional[str] = ' +0700',
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.target_table = target_table
        self.identifier = identifier
        self.postgres_conn = postgres_conn
        self.replace = replace
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.incremental_column = incremental_column
        self.mongo_collection = mongo_collection
        self.start_increment = start_increment
        self.end_increment = end_increment
        self.mongo_projection = mongo_projection
        self.new_column = new_column
        self.timezone = timezone

    def flatten(self, x):
        if isinstance(x, list):
            return [self.flatten(y) for y in x]
        elif isinstance(x, dict):
            return {key:self.flatten(value) for key, value in list(x.items())}
        elif isinstance(x, datetime):
            return str(x.isoformat()) + self.timezone
        elif str(x) == 'nan' or str(x) == 'NaT' or str(x) == 'None' or str(x)=='':
            return 'NULL'
        else:
            return str(x).replace("'",'"')

    def _serialize_cell(self, cell) -> str:
        """
        Returns the SQL literal of the cell as a string.

        :param cell: The cell to insert into the table
        :return: The serialized cell
        """

        if str(cell) == 'nan' or str(cell) == 'NaT' or str(cell) == 'None' or str(cell)=='':
            return 'NULL'
        if isinstance(cell, datetime):
            return "'" + str(cell.isoformat()) + self.timezone + "'"
        if isinstance(cell, list):
            new_list = []
            is_dict = False
            for lst in cell:

                lst = self.flatten(lst)

                if(isinstance(lst, dict)):
                    is_dict = True
                    new_list.append(json.dumps(lst))
                else:
                    new_list.append(str(lst).replace("'", '"'))

            if(str(is_dict)=='True'):
                cell = ','.join(new_list)
                return f"""'[{cell}]'"""
            else:
                cell = '","'.join(new_list)
                return f"""'["{cell}"]'"""

        if isinstance(cell, dict):
            for key, value in list(cell.items()):
                cell[key] = self.flatten(value)

            cell = '","'.join(cell)
            return f"""'["{cell}"]'"""
        if isinstance(cell, str):
            return "'" + str(cell).replace("'", "") + "'"
        return "'" + str(cell) + "'"

    @staticmethod
    def get_column(new_columns) -> str:

        if new_columns:
            target_fields_fragment = ", ".join(new_columns)
            target_fields_fragment = f"({target_fields_fragment})"
        else:
            target_fields_fragment = ""
        return target_fields_fragment

    def get_value(self, data, columns) -> str:
        values = ""

        for ind in data.index:
            lst = []
            for col, new_col in columns.items():
                cell = self._serialize_cell(data.get(col, {}).get(ind, None))
                lst.append(cell)

            lsj = '(' + ','.join(lst) + ')'
            values += lsj + ","
        values = values[:-1]

        return values

    def generate_query(self, data, columns) -> str:
        new_columns = []

        if self.new_column:
            # get new column name from config if any
            new_columns = [v for k,v in self.new_column.items()]
        else:
            new_columns.append(columns)

        # each document sometimes has different field, so we need get_value from specific field that we include
        # example return documents :
        # [{"_id": {
        #         "$oid": "63280d7acef45fd7be786ccc"},
        #          "name": "Mocachino"
        #          "has_active_status": true },
        #     {
        #         "_id": {"$oid": "633241bbdc69936e8464eb3b"},
        #         "name": "Kopi Hitam",
        #     }]

        sql = f"""
            INSERT INTO {self.target_table}
            {self.get_column(new_columns)}
            VALUES {self.get_value(data, self.new_column)}
        """

        if (str(self.replace)=='True'):
            if new_columns is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names")
            if self.identifier is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires an unique index")
            if isinstance(self.identifier, list):
                replace_index = ",".join(self.identifier)
            else:
                replace_index = self.identifier

            replace_target = [
                "{0} = excluded.{0}".format(col) for col in new_columns if col not in self.identifier
            ]

            sql += f"ON CONFLICT ({replace_index}) DO UPDATE SET {', '.join(replace_target)}"

        return sql

    def mongo_query(self) -> dict:
        isodate_start = datetime.strptime(self.start_increment, '%Y-%m-%dT%H:%M:%S%z')
        isodate_end = datetime.strptime(self.end_increment, '%Y-%m-%dT%H:%M:%S%z')

        columns = []
        if self.incremental_column:
            for column in self.incremental_column:
                columns.append(
                    {"$and": [
                        {column:
                             {"$gte": isodate_start}
                         },
                        {column:
                             {"$lte": isodate_end}
                         }]
                    }
                )

            query = {"$or": columns}

        else:
            query = {}

        return query

    def execute(self, context: 'Context') -> None:
        target = PostgresHook(self.postgres_conn, log_sql=True)
        source = MongoHook(self.mongo_conn_id)

        extract = source.find(
            mongo_collection=self.mongo_collection,
            query=cast(dict, self.mongo_query()),
            projection=self.mongo_projection,
            mongo_db=self.mongo_db,
        )

        # Insert data to pandas dataframe
        data = pd.DataFrame(extract)
        # Find column name based on query result
        columns = data.columns.tolist()
        # Perform inserting data
        if data.empty:
            self.log.info("There is no data to insert/update.")
        else:
            target.run(
                sql=self.generate_query(data, columns)
            )
