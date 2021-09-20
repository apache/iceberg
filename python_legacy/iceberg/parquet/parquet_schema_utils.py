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

from typing import List

from iceberg.api import Schema
from iceberg.api.types import get_projected_ids


def prune_columns(file_schema: Schema, expected_schema: Schema) -> List[str]:
    """
    Given two Iceberg schema's returns a list of column_names for all id's in the
    file schema that are projected in the expected schema

    Parameters
    ----------
    file_schema : iceberg.api.Schema
        An Iceberg schema of the file being read
    expected_schema : iceberg.api.Schema
        An Iceberg schema of the final projection
    Returns
    -------
    list
        The column names in the file that matched ids in the expected schema
    """
    return [column.name for column in file_schema.as_struct().fields
            if column.id in get_projected_ids(expected_schema)]
