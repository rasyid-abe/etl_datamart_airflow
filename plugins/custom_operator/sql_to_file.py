import enum
from collections import namedtuple
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence, Union

import json
import numpy as np
import pandas as pd
from typing_extensions import Literal

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
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

    """
    Saves data from a specific SQL query DB source into DB target.

    :param query: the sql query to be executed.
    :param sql_conn_source: reference to a specific connection of database source.
    :param parameters: (optional) the parameters to render the SQL query with.
    :param file_format: the destination file format, only string 'csv', 'json' or 'parquet' is accepted.
    :param sql_table: The MySQL table into where the data will be sent.
    :param sql_duplicate_key_handling: Specify what should happen to duplicate data.
        You can choose either `IGNORE` or `REPLACE`.

        .. seealso::
            https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-duplicate-key-handling
    :param sql_extra_options: MySQL options to specify exactly how to load the data.
    :param sql_conn_target: reference to a specific connection of database target.
    :param output: Define the output fle.
        You can choose either `S3_file` or `SQL_table`.
    :param s3_conn_id: reference to a specific S3 connection.
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
    :param s3_bucket: bucket where the data will be stored. (templated)
    :param s3_key: desired key for the file. It includes the name of the file. (templated)
    :param replace: whether or not to replace the file in S3 if it previously existed
    :param date_exec: desired executiin date for the file.
    :param column_value: default column value.
    :param pd_kwargs: arguments to include in DataFrame ``.to_parquet()``, ``.to_json()`` or ``.to_csv()``.
    """

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
            sql_conn_target: Optional[str] = None,
            parameters: Union[None, Mapping, Iterable] = None,
            output_query: str,
            coerce_float: Optional[bool] = True,
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
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.sql_conn_source = sql_conn_source
        self.sql_table = sql_table
        self.sql_duplicate_key_handling = sql_duplicate_key_handling
        self.sql_extra_options = sql_extra_options
        self.sql_conn_target = sql_conn_target
        self.verify = verify
        self.replace = replace
        self.output_query = output_query
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.date_exec = date_exec
        self.column_value = column_value
        self.pd_kwargs = pd_kwargs or {}
        self.parameters = parameters
        self.coerce_float = coerce_float
        self.cast_datatype = cast_datatype
        self.xcom_parameters = xcom_parameters
        self.xcom_formats = xcom_formats
        self.xcom_connectors = xcom_connectors

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

    def _get_file_path(self):
        file_path = self.s3_key + self.date_exec + "." + self.file_s3

        return file_path

    def get_load_query(self, data_df) -> str:
        """
        Get column from dataframe and build load SQL query
        """

        colums = data_df.columns.tolist()
        load_columns = ""
        for load_column in colums:
            load_columns += '@' + load_column + ", "
        load_columns = load_columns[:-2]

        load_filters = ""
        default_value = []
        for load_filter in colums:
            if self.column_value:
                for value in self.column_value:
                    if value['name'] == load_filter:
                        default_value.append("{0}=IF(@{0}='nan' OR @{0}='' OR @{0} IS NULL,{1},@{0}) ".format(load_filter, value['default']))
                    else:
                        default_value.append(
                            "{0}=IF(@{0}='nan' OR @{0}='',NULL,@{0}) ".format(load_filter))
            else:
                default_value.append(
                    "{0}=IF(@{0}='nan' OR @{0}='',NULL,@{0}) ".format(load_filter))

        load_filters += ', '.join(default_value)

        mysql_load = f"""
            FIELDS TERMINATED BY ','
            ENCLOSED BY '\"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (@dummy, {load_columns})
            SET {load_filters}
        """

        return mysql_load

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

        sql_hook = MySqlHook(mysql_conn_id=self.sql_conn_source)
        # store to pandas dataframe
        self.log.info("query : " + self.query)
        data_df = sql_hook.get_pandas_df(sql=self.query, parameters=self.parameters, coerce_float=self.coerce_float)

        if data_df.empty:
            self.log.info("data is empty")
        else:
            self.log.info("Data from SQL obtained")
            # cleansing pandas dataframe
            self._fix_dtypes(data_df, self.cast_datatype)
            file_options = FILE_OPTIONS_MAP[self.file_format]

            with NamedTemporaryFile(mode=file_options.mode, suffix=file_options.suffix) as tmp_file:

                self.log.info("Writing data to temp file")
                getattr(data_df, file_options.function)(tmp_file.name, **self.pd_kwargs)

                if self.output_query == 'SQL_table':
                    self.log.info("Load data to SQL table")
                    mysql_hook = MySqlHook(mysql_conn_id=self.sql_conn_target)
                    if self.sql_extra_options:
                        load_query = self.sql_extra_options
                        self.log.info("Load query : " + self.sql_extra_options)
                    else:
                        load_query = self.get_load_query(data_df)
                        self.log.info("Load query : " + load_query)
                    mysql_hook.bulk_load_custom(
                        table=self.sql_table,
                        tmp_file=tmp_file.name,
                        duplicate_key_handling=self.sql_duplicate_key_handling,
                        extra_options=load_query
                    )
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
