#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence, Union, Callable

from psycopg2.sql import SQL, Identifier

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.www import utils as wwwutils

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PostgresOperatorWithHandler(BaseOperator):
    """
    Executes sql code in a specific Postgres database, with additional option 'handler' to handle result of this operator.
    This operator imitate flow PostgresOperator in airflow version 2.4.*

    Executes sql code in a specific Postgres database

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param database: name of database which overwrite defined one in connection
    :param handler: (optional) the function that will be applied to the cursor (default: None).
    """

    template_fields: Sequence[str] = ('sql', 'parameters')
    # TODO: Remove renderer check when the provider has an Airflow 2.3+ requirement.
    template_fields_renderers = {
        'sql': 'postgresql' if 'postgresql' in wwwutils.get_attr_renderer() else 'sql',
        'parameters': 'json',
    }
    template_ext: Sequence[str] = ('.sql', '.json')
    ui_color = '#ededed'

    def __init__(
            self,
            *,
            sql: Union[str, Iterable[str]],
            postgres_conn_id: str = 'postgres_default',
            autocommit: bool = False,
            parameters: Optional[Union[Iterable, Mapping]] = None,
            database: Optional[str] = None,
            runtime_parameters: Optional[Mapping] = None,
            handler: Optional[Callable] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.runtime_parameters = runtime_parameters
        self.hook: Optional[PostgresHook] = None
        self.handler = handler

    def execute(self, context: 'Context'):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        if self.runtime_parameters:
            final_sql = []
            sql_param = {}
            for param in self.runtime_parameters:
                set_param_sql = f"SET {{}} TO %({param})s;"
                dynamic_sql = SQL(set_param_sql).format(Identifier(f"{param}"))
                final_sql.append(dynamic_sql)
            for param, val in self.runtime_parameters.items():
                sql_param.update({f"{param}": f"{val}"})
            if self.parameters:
                sql_param.update(self.parameters)
            if isinstance(self.sql, str):
                final_sql.append(SQL(self.sql))
            else:
                final_sql.extend(list(map(SQL, self.sql)))
            self.hook.run(final_sql, self.autocommit, parameters=sql_param)
        if self.do_xcom_push:
            output = self.hook.run(
                sql=self.sql,
                autocommit=self.autocommit,
                handler=self.handler,
                parameters=self.parameters
            )
        else:
            output = self.hook.run(
                sql=self.sql,
                autocommit=self.autocommit,
                parameters=self.parameters
            )

        return output
