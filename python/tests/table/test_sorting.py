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
from pyiceberg import transforms
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder, UNSORTED_SORT_ORDER,
)
from pyiceberg.transforms import IdentityTransform, VoidTransform
from pyiceberg.types import IntegerType, StringType


def test_serialize_sort_order_unsorted():
    assert UNSORTED_SORT_ORDER.json() == '{"order-id": 0, "fields": []}'

def test_serialize_sort_order():
    sort_order = SortOrder(
        22,
        SortField(source_id=19, transform=IdentityTransform(StringType()), null_order=NullOrder.NULLS_FIRST),
        SortField(source_id=25, transform=transforms.bucket(IntegerType(), 4), direction=SortDirection.DESC),
        SortField(source_id=22, transform=VoidTransform(), direction=SortDirection.ASC),
    )
    expected = '{"order-id": 22, "fields": [{"source-id": 19, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}, {"source-id": 25, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"}, {"source-id": 22, "transform": "void", "direction": "asc", "null-order": "nulls-first"}]}'
    assert sort_order.json() == expected


def test_deserialize_sort_order():
    expected = SortOrder(
        22,
        SortField(source_id=19, transform=transforms.identity, null_order=NullOrder.NULLS_FIRST),
        SortField(source_id=25, transform=transforms.bucket, direction=SortDirection.DESC),
        SortField(source_id=22, transform=transforms.always_null, direction=SortDirection.ASC),
    )
    payload = '{"order-id": 22, "fields": [{"source-id": 19, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}, {"source-id": 25, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"}, {"source-id": 22, "transform": "void", "direction": "asc", "null-order": "nulls-first"}]}'

    assert SortOrder.parse_raw(payload) == expected
