import enum
from collections import namedtuple
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Optional, Sequence, Union, cast

import numpy as np
import pandas as pd
from typing_extensions import Literal
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FILE_FORMAT(enum.Enum):
    """Possible file formats."""

    CSV = enum.auto()
    JSON = enum.auto()


FileOptions = namedtuple('FileOptions', ['mode', 'suffix', 'function'])

FILE_OPTIONS_MAP = {
    FILE_FORMAT.CSV: FileOptions('r+', '.csv', 'to_csv'),
    FILE_FORMAT.JSON: FileOptions('r+', '.json', 'to_json'),
}


class MongoToMySqlOperator(BaseOperator):
    """
    Saves data from a specific Mongo query DB source into SQL DB target.

    :param file_format: the destination file format, only string 'csv' or 'json' is accepted.
    :param sql_table: The MySQL table into where the data will be sent.
    :param sql_duplicate_key_handling: Specify what should happen to duplicate data.
        You can choose either `IGNORE` or `REPLACE`.

        .. seealso::
            https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-duplicate-key-handling
    :param sql_extra_options: MySQL options to specify exactly how to load the data.
    :param sql_conn_target: reference to a specific connection of database target.
    :param date_exec: desired executiin date for the file.
    :param pd_kwargs: arguments to include in DataFrame ``.to_parquet()``, ``.to_json()`` or ``.to_csv()``.
    :param mongo_conn_id: reference to a specific mongo connection
    :param mongo_collection: reference to a specific collection in your mongo db
    :param mongo_query: query to execute. A list including a dict of the query
    :param mongo_projection: optional parameter to filter the returned fields by
        the query. It can be a list of fields names to include or a dictionary
        for excluding fields (e.g ``projection={"_id": 0}`` )
    :param mongo_db: reference to a specific mongo database
    :param incremental_column: reference to a specific column that represent change state
    :param start_increment: reference to a start date
    :param end_increment: reference to a end date
    :param allow_disk_use: enables writing to temporary files in the case you are handling large dataset.
        This only takes effect when `mongo_query` is a list - running an aggregate pipeline
    :param new_column: reference to a column with new name
    :param to_string: reference to a column with convert column to string
    """

    template_fields: Sequence[str] = (
        'sql_table',
        'date_exec',
        'mongo_query',
        'mongo_collection',
        'start_date',
        'end_date',
        'start_increment',
        'end_increment'
    )
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {
        "pd_kwargs": "json",
    }

    def __init__(
            self,
            *,
            sql_table: Optional[str] = None,
            sql_duplicate_key_handling: str = 'IGNORE',
            sql_extra_options: Optional[str] = None,
            sql_conn_target: Optional[str] = None,
            file_format: Literal['csv', 'json'] = 'csv',
            date_exec: Optional[str] = None,
            pd_kwargs: Optional[dict] = None,
            mongo_conn_id: str = None,
            mongo_collection: Optional[str] = None,
            mongo_query: Union[list, dict] = None,
            mongo_db: Optional[str] = None,
            mongo_projection: Optional[Union[list, dict]] = None,
            incremental_column: list,
            start_increment: str,
            end_increment: str,
            allow_disk_use: bool = False,
            new_column: dict,
            to_string: list = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql_table = sql_table
        self.sql_duplicate_key_handling = sql_duplicate_key_handling
        self.sql_extra_options = sql_extra_options or ''
        self.sql_conn_target = sql_conn_target
        self.file_format = file_format
        self.date_exec = date_exec
        self.pd_kwargs = pd_kwargs or {}
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.incremental_column = incremental_column
        self.mongo_collection = mongo_collection
        self.mongo_query = mongo_query
        self.start_increment = start_increment
        self.end_increment = end_increment
        self.is_pipeline = isinstance(self.mongo_query, list)
        self.mongo_projection = mongo_projection
        self.allow_disk_use = allow_disk_use
        self.new_column = new_column
        self.to_string = to_string

        if "path_or_buf" in self.pd_kwargs:
            raise AirflowException('The argument path_or_buf is not allowed, please remove it')

        try:
            self.file_format = FILE_FORMAT[file_format.upper()]
        except KeyError:
            raise AirflowException(f"The argument file_format doesn't support {file_format} value.")

    def _fix_dtypes(self, df) -> None:
        """
        Mutate DataFrame to set dtypes for float columns containing NaN values.
        Set dtype of object to str to allow for downstream transformations.
        """
        df.fillna(value=np.nan, inplace=True)
        df.replace("None", np.nan, inplace=True)

        for col in df:

            if df[col].dtype.name == 'object':
                # convert from list to string
                if self.to_string:
                    for fld in self.to_string:
                        if fld == col:
                            df[col] = df[col].dropna().apply(lambda x: ', '.join(map(str, x)))
                # if the type wasn't identified or converted, change it to a string so if can still be processed.
                df[col] = df[col].astype(str)

            if "float" in df[col].dtype.name and df[col].hasnans:
                # inspect values to determine if dtype of non-null values is int or float
                notna_series = df[col].dropna().values
                if np.equal(notna_series, notna_series.astype(int)).all():
                    # set to dtype that retains integers and supports NaNs
                    df[col] = np.where(df[col].isnull(), None, df[col])
                    df[col] = df[col].astype(pd.Int64Dtype())
                elif np.isclose(notna_series, notna_series.astype(int)).all():
                    # set to float dtype that retains floats and supports NaNs
                    df[col] = np.where(df[col].isnull(), None, df[col])
                    df[col] = df[col].astype(pd.Float64Dtype())

    def load_query(self, df) -> str:
        """
        Get column from dataframe and build load SQL query
        """

        colums = df.columns.tolist()
        load_columns = ""
        for load_column in colums:
            for k, v in self.new_column.items():
                if k == load_column:
                    load_columns += '@' + v + ", "
        load_columns = load_columns[:-2]

        load_filters = ""
        for load_filter in colums:
            for k, v in self.new_column.items():
                if k == load_filter:
                    load_filters += (v + " = IF(@" + v + "='nan' OR @" + v + "='' OR @" + v + "<='1970-01-01 00:00:00.000', NULL, @" + v + "), ")
        load_filters = load_filters[:-2]

        mysql_load = """
            FIELDS TERMINATED BY ','
            ENCLOSED BY '\"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (@dummy, """ + load_columns + """)
            SET """ + load_filters

        return mysql_load

    def extract_query(self) -> dict:
        isodate_start = datetime.strptime(self.start_increment, '%Y-%m-%dT%H:%M:%S%z')
        isodate_end = datetime.strptime(self.end_increment, '%Y-%m-%dT%H:%M:%S%z')

        columns = []
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

        return query

    def execute(self, context: 'Context') -> None:
        """Is written to depend on transform method"""

        # Grab collection and execute query according to whether or not it is a list
        query = MongoHook(self.mongo_conn_id).find(
            mongo_collection=self.mongo_collection,
            query=cast(dict, self.extract_query()),
            projection=self.mongo_projection,
            mongo_db=self.mongo_db,
        )

        mongo_df = pd.DataFrame(query)
        self.log.info("Mongo query : ")
        self.log.info(self.extract_query())

        if mongo_df.empty:
            self.log.info("data is empty")
        else:
            self._fix_dtypes(mongo_df)
            file_options = FILE_OPTIONS_MAP[self.file_format]

            with NamedTemporaryFile(mode=file_options.mode, suffix=file_options.suffix) as tmp_file:
                self.log.info("Writing data to temp file")
                getattr(mongo_df, file_options.function)(tmp_file.name, **self.pd_kwargs)
                self.log.info("Load data to SQL table")
                self.log.info("Load query : " + self.load_query(mongo_df))
                mysql_hook = MySqlHook(mysql_conn_id=self.sql_conn_target)
                mysql_hook.bulk_load_custom(
                    table=self.sql_table,
                    tmp_file=tmp_file.name,
                    duplicate_key_handling=self.sql_duplicate_key_handling,
                    extra_options=self.load_query(mongo_df)
                )
