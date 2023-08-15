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
# pylint:disable=redefined-outer-name,eval-used
import json
from typing import Any, Dict

import pytest

from pyiceberg.table.metadata import TableMetadataUtil
from pyiceberg.table.sorting import (
    UNSORTED_SORT_ORDER,
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import BucketTransform, IdentityTransform, VoidTransform


@pytest.fixture
def sort_order() -> SortOrder:
    return SortOrder(
        SortField(source_id=19, transform=IdentityTransform(), null_order=NullOrder.NULLS_FIRST),
        SortField(source_id=25, transform=BucketTransform(4), direction=SortDirection.DESC),
        SortField(source_id=22, transform=VoidTransform(), direction=SortDirection.ASC),
        order_id=22,
    )


def test_serialize_sort_order_unsorted() -> None:
    assert UNSORTED_SORT_ORDER.model_dump_json() == '{"order-id":0,"fields":[]}'


def test_serialize_sort_order(sort_order: SortOrder) -> None:
    expected = '{"order-id":22,"fields":[{"source-id":19,"transform":"identity","direction":"asc","null-order":"nulls-first"},{"source-id":25,"transform":"bucket[4]","direction":"desc","null-order":"nulls-last"},{"source-id":22,"transform":"void","direction":"asc","null-order":"nulls-first"}]}'
    assert sort_order.model_dump_json() == expected


def test_deserialize_sort_order(sort_order: SortOrder) -> None:
    payload = '{"order-id": 22, "fields": [{"source-id": 19, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}, {"source-id": 25, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"}, {"source-id": 22, "transform": "void", "direction": "asc", "null-order": "nulls-first"}]}'

    assert SortOrder.model_validate_json(payload) == sort_order


def test_sorting_schema(example_table_metadata_v2: Dict[str, Any]) -> None:
    table_metadata = TableMetadataUtil.parse_raw(json.dumps(example_table_metadata_v2))

    assert table_metadata.sort_orders == [
        SortOrder(
            SortField(2, IdentityTransform(), SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
            SortField(
                3,
                BucketTransform(4),
                direction=SortDirection.DESC,
                null_order=NullOrder.NULLS_LAST,
            ),
            order_id=3,
        )
    ]


def test_sorting_to_string(sort_order: SortOrder) -> None:
    expected = """[
  19 ASC NULLS FIRST
  bucket[4](25) DESC NULLS LAST
  void(22) ASC NULLS FIRST
]"""
    assert str(sort_order) == expected


def test_sorting_to_repr(sort_order: SortOrder) -> None:
    expected = """SortOrder(SortField(source_id=19, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST), SortField(source_id=25, transform=BucketTransform(num_buckets=4), direction=SortDirection.DESC, null_order=NullOrder.NULLS_LAST), SortField(source_id=22, transform=VoidTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST), order_id=22)"""
    assert repr(sort_order) == expected


def test_unsorting_to_repr() -> None:
    expected = """SortOrder(order_id=0)"""
    assert repr(UNSORTED_SORT_ORDER) == expected


def test_sorting_repr(sort_order: SortOrder) -> None:
    """To make sure that the repr converts back to the original object"""
    assert sort_order == eval(repr(sort_order))
