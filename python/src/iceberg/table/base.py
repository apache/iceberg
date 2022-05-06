#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from __future__ import annotations

from abc import ABC
from typing import Dict

from attrs import Factory, field, frozen

from iceberg.schema import Schema


@frozen(kw_only=True)
class TableSpec:
    """An immutable user specification to create or replace a table.

    Usage:
        table_spec = TableSpec(
            namespace = "com.organization.department",
            name = "my_table",
            schema = Schema(),
            location = "protocol://some/location",  // Optional
            partition_spec = PartitionSpec(),       // Optional
            properties = [                          // Optional
                "key1": "value1",
                "key2": "value2",
            ]
        )

    TODO: Validators to be added
    """

    _namespace: str
    _name: str
    _schema: Schema
    _location: str = field()
    _partition_spec: PartitionSpec = field()
    _properties: Dict[str, str] = Factory(Dict[str, str])


class Table(ABC):
    """Placeholder for Table managed by the Catalog that points to the current Table Metadata.

    To be implemented by https://github.com/apache/iceberg/issues/3227
    """


class PartitionSpec:
    """Placeholder for Partition Specification

    To be implemented by https://github.com/apache/iceberg/issues/4631
    """
