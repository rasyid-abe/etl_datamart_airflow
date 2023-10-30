import enum
from collections import namedtuple
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence, Union

from contextlib import closing

import json
import numpy as np
import pandas as pd
from typing_extensions import Literal

import re
import csv

from psycopg2.extras import execute_values

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FILE_FORMAT(enum.Enum):
    """Possible file formats."""

    CSV = enum.auto()
    JSON = enum.auto()
    PARQUET = enum.auto()


FileOptions = namedtuple('FileOptions', ['mode', 'suffix', 'function'])

FILE_OPTIONS_MAP = {
    FILE_FORMAT.CSV: FileOptions('r+', '.csv', 'to_csv'),
    FILE_FORMAT.JSON: FileOptions('r+', '.json', 'to_json'),
    FILE_FORMAT.PARQUET: FileOptions('rb+', '.parquet', 'to_parquet'),
}


class SqlToFileOperator(BaseOperator):

    template_fields: Sequence[str] = (
        'query',
        'sql_table',
        'date_exec',
        'xcom_parameters'
    )
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {
        "query": "sql",
        "pd_kwargs": "json",
    }

    def __init__(
            self,
            *,
            query: str,
            sql_conn_source: str,
            sql_table: Optional[str] = None,
            sql_duplicate_key_handling: str = 'IGNORE',
            sql_extra_options: Optional[str] = None,
            delimiter: Optional[str] = ',',
            sql_conn_target: Optional[str] = None,
            db_source: Optional[str] = 'mysql',
            parameters: Union[None, Mapping, Iterable] = None,
            output_query: str,
            coerce_float: Optional[bool] = True,
            on_conflict: Optional[list] = None,
            ignore_columns : Optional[list] = None,
            conflict_action: Optional[list] = 'DO NOTHING',
            s3_conn_id: Optional[str] = None,
            verify: Optional[Union[bool, str]] = None,
            replace: Optional[bool] = False,
            s3_bucket: Optional[str] = None,
            s3_key: Optional[str] = None,
            file_format: Literal['csv', 'json', 'parquet'] = 'csv',
            date_exec: Optional[str] = None,
            column_value: Optional[list] = None,
            pd_kwargs: Optional[dict] = None,
            cast_datatype: Optional[Union[list, dict]] = {},
            xcom_parameters : Optional[list] = [],
            xcom_formats : Optional[list] = [],
            xcom_connectors : Optional[list] = [],
            clean_data_by_regex : re = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.sql_conn_source = sql_conn_source
        self.sql_table = sql_table
        self.sql_duplicate_key_handling = sql_duplicate_key_handling
        self.sql_extra_options = sql_extra_options
        self.delimiter = delimiter
        self.sql_conn_target = sql_conn_target
        self.verify = verify
        self.ignore_columns = ignore_columns
        self.replace = replace
        self.output_query = output_query
        self.on_conflict = on_conflict
        self.conflict_action = conflict_action
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.date_exec = date_exec
        self.column_value = column_value
        self.pd_kwargs = pd_kwargs or {}
        self.db_source = db_source
        self.parameters = parameters
        self.coerce_float = coerce_float
        self.cast_datatype = cast_datatype
        self.xcom_parameters = xcom_parameters
        self.xcom_formats = xcom_formats
        self.xcom_connectors = xcom_connectors
        self.clean_data_by_regex = clean_data_by_regex

        if "path_or_buf" in self.pd_kwargs:
            raise AirflowException('The argument path_or_buf is not allowed, please remove it')

        self.file_s3 = ""
        for file in self.file_format:
            self.file_s3 += file
        try:
            self.file_format = FILE_FORMAT[file_format.upper()]
        except KeyError:
            raise AirflowException(f"The argument file_format doesn't support {file_format} value.")

            # raise AirflowException("The argumen xcom_parameters doesn't have valid format & connectors!")


    @staticmethod
    def _fix_dtypes(df: pd.DataFrame, cast={}) -> None:
        """
        Mutate DataFrame to set dtypes for float columns containing NaN values.
        Set dtype of object to str to allow for downstream transformations.
        """
        df.fillna(value=np.nan, inplace=True)
        df.replace("None", np.nan, inplace=True)

        for col in df:

            if df[col].dtype.name == 'object':
                # if the type wasn't identified or converted, change it to a string so if can still be
                # processed.
                # remove null character and return carriage character.
                # if in future error still persist, debug the line and replace character that caused error

                re_null = re.compile(pattern='\x00')
                df[col].replace(regex=re_null, value=' ', inplace=True)

                re_return = re.compile(pattern='\r')
                df[col].replace(regex=re_return, value='', inplace=True)

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

        for col in cast:
            if(cast[col] == "bigint"):
                df[col] = df[col].astype("float").astype(pd.Int64Dtype())
            elif(cast[col] =="int"):
                df[col] = df[col].astype("float").astype(pd.Int32Dtype())
            elif(cast[col]=="smallint" or cast[col] == "tinyint"):
                df[col] = df[col].astype("float").astype(pd.Int16Dtype())
            elif(cast[col] == "boolean" or cast[col] == "bool"):
                df[col] = df[col].astype(bool)

    def _get_file_path(self):
        file_path = self.s3_key + self.date_exec + "." + self.file_s3

        return file_path

    def execute(self, context: 'Context') -> None:

        if(len(self.xcom_parameters) != 0 ):
            if(len(self.xcom_parameters) != len(self.xcom_formats) and len(self.xcom_parameters) != len(self.xcom_connectors)):
                raise AirflowException("The argument xcom_parameters doesn't have valid format & connectors!")

            self.log.info(self.xcom_parameters)
            try:
                self.log.info("Parsing xcom parameters!")
                sql_xcom_parameters = []
                for index,jsonstring in enumerate(self.xcom_parameters):
                    parameters = json.loads(jsonstring)
                    holder = []
                    for item in parameters:
                        if(isinstance(item, list)):
                            holder.append(self.xcom_formats[index].format(*item))
                        else:
                            holder.append(self.xcom_formats[index].format(item))
                    sql_xcom_parameters.append( (" {} ".format(self.xcom_connectors[index])).join(holder) )
                self.query = self.query.format(*sql_xcom_parameters)
            except Exception as e:
                raise AirflowException("The argument xcom_parameters doesn't have valid format & connectors!")

        if self.db_source == 'mysql':
            sql_hook = MySqlHook(mysql_conn_id=self.sql_conn_source)
        else:
            sql_hook = PostgresHook(self.sql_conn_source)
        # store to pandas dataframe
        self.log.info("query : " + self.query)

        data_df = sql_hook.get_pandas_df(sql=self.query, parameters=self.parameters, coerce_float=self.coerce_float)

        if self.ignore_columns:
            data_df.drop(self.ignore_columns, inplace=True, axis=1)

        if data_df.empty:
            self.log.info("data is empty")
        else:
            self.log.info("Data from SQL obtained")
            # cleansing pandas dataframe

            if(self.clean_data_by_regex != None):
                # after research the best practice to remove linebreaks & return & tabs is using this regex.
                self.log.info(f"clean data using this regex : {self.clean_data_by_regex}")
                data_df.replace(self.clean_data_by_regex,' ',regex=True, inplace=True)

            self._fix_dtypes(data_df, self.cast_datatype)
            file_options = FILE_OPTIONS_MAP[self.file_format]

            with NamedTemporaryFile(mode=file_options.mode, suffix=file_options.suffix) as tmp_file:

                self.log.info("Writing data to temp file")
                getattr(data_df, file_options.function)(path_or_buf = tmp_file.name, sep=self.delimiter, header=False, index=False, quoting=csv.QUOTE_MINIMAL)

                if self.output_query == 'SQL_table':
                    self.log.info("Load data to SQL table")
                    psql_hook = PostgresHook(self.sql_conn_target)

                    tmp_name = str(tmp_file.name).split('/')[-1].split('.')[0]

                    with closing(psql_hook.get_conn()) as conn:
                        with closing(conn.cursor()) as cur:
                            # create temp table
                            temp_table_query = \
                            f"""
                            CREATE
                                TEMP TABLE {tmp_name}
                            AS
                                SELECT
                                    *
                                FROM
                                    {self.sql_table} WITH NO DATA
                            """
                            conn = psql_hook.get_conn()
                            cur = conn.cursor()
                            cur.execute(temp_table_query)
                            conn.commit()

                            # insert file data into temp table
                            query = f"{tmp_name}  {*data_df,}".replace("'",'').replace('\r','' ).replace("\n",'')
                            query = f"COPY {query} FROM STDIN WITH CSV delimiter '{self.delimiter}'"
                            with open(tmp_file.name, "r+") as file:
                                cur.copy_expert(query, file)
                                file.truncate(file.tell())
                                conn.commit()

                            # insert main table using temp table
                            insert_query = \
                            f"""
                                INSERT INTO
                                    {self.sql_table}
                                    SELECT
                                        *
                                    FROM
                                        {tmp_name}
                                ON CONFLICT DO NOTHING
                            """
                            cur.execute(insert_query)
                            cur.execute(f'DROP TABLE {tmp_name}')
                            conn.commit()
                else:
                    self.log.info("Uploading data to S3")
                    s3_conn = S3Hook(aws_conn_id=self.s3_conn_id, verify=self.verify)
                    s3_conn.load_file(
                        filename=tmp_file.name,
                        key=self._get_file_path(),
                        bucket_name=self.s3_bucket,
                        replace=self.replace
                    )
                    self.log.info("data uploaded into : " + self._get_file_path())
