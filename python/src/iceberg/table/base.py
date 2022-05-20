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
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from iceberg.schema import Schema
from iceberg.table.order import SortOrder

Identifier = Tuple[str, ...]
Properties = Dict[str, str]


class Table(ABC):
    """Placeholder for Table managed by the Catalog that points to the current Table Metadata.

    To be implemented by https://github.com/apache/iceberg/issues/3227
    """


class PartitionSpec:
    """Placeholder for Partition Specification

    To be implemented by https://github.com/apache/iceberg/issues/4631
    """


@dataclass(frozen=True)
class TableBuilder:
    """A builder used to create valid tables or start create/replace transactions.

    Usage:
        table = TableBuilder(
            identifier = ('com','organization','department','my_table'),
            schema = Schema(schema_id=1),
            location = "protocol://some/location",  // Optional
            partition_spec = PartitionSpec(),       // Optional
            sort_order = SortOrder(),               // Optional
            properties = [                          // Optional
                "key1": "value1",
                "key2": "value2",
            ]
        )
        .create()
    """

    identifier: Identifier
    schema: Schema
    location: Optional[str] = field(default=None)
    partition_spec: Optional[PartitionSpec] = field(default=None)
    sort_order: Optional[SortOrder] = field(default=None)
    properties: Properties = field(default_factory=dict)

    def __post_init__(self):
        """Validates the table builder.

        Raises:
            ValueError: If the instance is in an invalid state
        """

    def create(self) -> Table:
        """Creates the table.

        Returns:
            Table: the created table

        Raises:
            AlreadyExistsError: If a table with the given identifier already exists
        """
