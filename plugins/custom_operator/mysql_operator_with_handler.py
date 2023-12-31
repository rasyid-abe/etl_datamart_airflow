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
from __future__ import annotations

import ast
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence, Optional, Callable

from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.www import utils as wwwutils

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MysqlOperatorWithHandler(BaseOperator):
    """
    Executes sql code in a specific MySQL database, with additional option 'handler' to handle result of this operator.
    This operator imitate flow MysqlOperator in airflow version 2.4.*

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MySqlOperator`

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :param mysql_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
    :param parameters: (optional) the parameters to render the SQL query with.
        Template reference are recognized by str ending in '.json'
        (templated)
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param database: name of database which overwrite defined one in connection
    :param handler: (optional) the function that will be applied to the cursor (default: None).
    """

    template_fields: Sequence[str] = ('sql', 'parameters')
    # TODO: Remove renderer check when the provider has an Airflow 2.3+ requirement.
    template_fields_renderers = {
        'sql': 'mysql' if 'mysql' in wwwutils.get_attr_renderer() else 'sql',
        'parameters': 'json',
    }
    template_ext: Sequence[str] = ('.sql', '.json')
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: str | Iterable[str],
        mysql_conn_id: str = 'mysql_default',
        parameters: Iterable | Mapping | None = None,
        autocommit: bool = False,
        database: str | None = None,
        handler: Optional[Callable] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.handler = handler

    def prepare_template(self) -> None:
        """Parse template file for attribute parameters."""
        if isinstance(self.parameters, str):
            self.parameters = ast.literal_eval(self.parameters)

    def execute(self, context: Context) -> None:
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
        if self.do_xcom_push:
            output = hook.run(
                sql=self.sql,
                autocommit=self.autocommit,
                parameters=self.parameters,
                handler=self.handler
            )
        else:
            output = hook.run(
                sql=self.sql,
                autocommit=self.autocommit,
                parameters=self.parameters
            )

        return output