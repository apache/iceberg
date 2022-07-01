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
    SortOrder,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, StringType


def test_serialize_sort_order():
    sort_order = SortOrder(
        22,
        SortField(source_id=19, transform=IdentityTransform(StringType()), null_order=NullOrder.NULLS_FIRST),
        SortField(source_id=25, transform=transforms.bucket(IntegerType(), 4), direction=SortDirection.DESC),
    )
    assert (
        sort_order.json()
        == """{
      "order-id": 3,
      "fields": [
        {
          "transform": "identity",
          "source-id": 19,
          "direction": "asc",
          "null-order": "nulls-first"
        },
        {
          "transform": "bucket[4]",
          "source-id": 25,
          "direction": "desc",
          "null-order": "nulls-last"
        }
      ]
    }"""
    )
