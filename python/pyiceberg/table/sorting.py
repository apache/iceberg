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
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

from pydantic import Field, root_validator

from pyiceberg.transforms import Transform
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class NullOrder(str, Enum):
    NULLS_FIRST = "nulls-first"
    NULLS_LAST = "nulls-last"


class SortField(IcebergBaseModel):
    def __init__(
        self,
        source_id: int,
        transform: Transform,
        direction: Optional[SortDirection] = None,
        null_order: Optional[NullOrder] = None,
        **data: Any,
    ):
        data["source-id"] = source_id
        data["transform"] = transform
        data["direction"] = direction
        data["null-order"] = null_order
        super().__init__(**data)

    @root_validator(pre=True)
    def set_null_order(self, values: Dict[str, Any]) -> Dict[str, Any]:
        values["direction"] = values["direction"] if values["direction"] else SortDirection.ASC
        if not values["null_order"]:
            values["direction"] = NullOrder.NULLS_FIRST if values["direction"] == SortDirection.ASC else NullOrder.NULLS_LAST
        return values

    source_id: int = Field(alias="source-id")
    transform: Transform = Field()
    direction: SortDirection = Field()
    null_order: NullOrder = Field(alias="null-order")


class SortOrder(IcebergBaseModel):
    """
    Users can sort their data within partitions by columns to gain performance.

    The order of the sort fields within the list defines the order in which the sort is applied to the data.

    Each sort field consists of:

    - Source column id from the table’s schema
    - Transform that is used to produce values to be sorted on from the source column.
      This is the same transform as described in partition transforms.
    - Sort direction, that can only be either asc or desc
    - Null order that describes the order of null values when sorted. Can only be either nulls-first or nulls-last
    """

    def __init__(self, order_id: int, *fields: SortField, **data: Any):
        data["order-id"] = order_id
        if fields:
            data["fields"] = fields
        super().__init__(**data)

    order_id: Optional[int] = Field(alias="order-id")
    fields: List[SortField] = Field(default_factory=list)


UNSORTED_SORT_ORDER_ID = 0
UNSORTED_SORT_ORDER = SortOrder(order_id=UNSORTED_SORT_ORDER_ID)
