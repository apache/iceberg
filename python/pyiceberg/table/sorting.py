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
# pylint: disable=keyword-arg-before-vararg
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
)

from pydantic import Field, root_validator

from pyiceberg.transforms import Transform, IdentityTransform
from pyiceberg.types import IcebergType
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel


class SortDirection(Enum):
    ASC = "asc"
    DESC = "desc"

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"SortDirection.{self.name}"


class NullOrder(Enum):
    NULLS_FIRST = "nulls-first"
    NULLS_LAST = "nulls-last"

    def __str__(self) -> str:
        return self.name.replace("_", " ")

    def __repr__(self) -> str:
        return f"NullOrder.{self.name}"


class SortField(IcebergBaseModel):
    """Sort order field

    Args:
      source_id (int): Source column id from the table’s schema
      transform (str): Transform that is used to produce values to be sorted on from the source column.
                       This is the same transform as described in partition transforms.
      direction (SortDirection): Sort direction, that can only be either asc or desc
      null_order (NullOrder): Null order that describes the order of null values when sorted. Can only be either nulls-first or nulls-last
    """

    def __init__(
        self,
        source_id: Optional[int] = None,
        transform: Optional[Union[Transform, Callable[[IcebergType], Transform]]] = None,
        direction: Optional[SortDirection] = None,
        null_order: Optional[NullOrder] = None,
        **data: Any,
    ):
        if source_id is not None:
            data["source-id"] = source_id
        if transform is not None:
            data["transform"] = transform
        if direction is not None:
            data["direction"] = direction
        if null_order is not None:
            data["null-order"] = null_order
        super().__init__(**data)

    @root_validator(pre=True)
    def set_null_order(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["direction"] = values["direction"] if values.get("direction") else SortDirection.ASC
        if not values.get("null-order"):
            values["null-order"] = NullOrder.NULLS_FIRST if values["direction"] == SortDirection.ASC else NullOrder.NULLS_LAST
        return values

    source_id: int = Field(alias="source-id")
    transform: Transform = Field()
    direction: SortDirection = Field()
    null_order: NullOrder = Field(alias="null-order")

    def __str__(self):
        if type(self.transform) == IdentityTransform:
            # In the case of an identity transform, we can omit the transform
            return f"{self.source_id} {self.direction} {self.null_order}"
        else:
            return f"{self.transform}({self.source_id}) {self.direction} {self.null_order}"


class SortOrder(IcebergBaseModel):
    """Describes how the data is sorted within the table

    Users can sort their data within partitions by columns to gain performance.

    The order of the sort fields within the list defines the order in which the sort is applied to the data.

    Args:
      order_id (int): The id of the sort-order. To keep track of historical sorting
      fields (List[SortField]): The fields how the table is sorted
    """

    def __init__(self, order_id: Optional[int] = None, *fields: SortField, **data: Any):
        if order_id is not None:
            data["order-id"] = order_id
        if fields:
            data["fields"] = fields
        super().__init__(**data)

    order_id: Optional[int] = Field(alias="order-id")
    fields: List[SortField] = Field(default_factory=list)

    def __str__(self) -> str:
        result_str = "["
        if self.fields:
            result_str += "\n  " + "\n  ".join([str(field) for field in self.fields]) + "\n"
        result_str += "]"
        return result_str


UNSORTED_SORT_ORDER_ID = 0
UNSORTED_SORT_ORDER = SortOrder(order_id=UNSORTED_SORT_ORDER_ID)
