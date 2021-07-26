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

import unittest

from iceberg.api.types import (BooleanType, NestedField, StructType)
from iceberg.api.types import type_util
from iceberg.exceptions import ValidationException


class TestConversions(unittest.TestCase):

    def test_validate_schema_via_index_by_name(self):
        nested_type = NestedField.required(
            1, "a", StructType.of(
                [NestedField.required(2, "b", StructType.of(
                    [NestedField.required(3, "c", BooleanType.get())])),
                 NestedField.required(4, "b.c", BooleanType.get())]))

        with self.assertRaises(ValidationException) as context:
            type_util.index_by_name(StructType.of([nested_type]))
        self.assertTrue('Invalid schema: multiple fields for name a.b.c: 3 and 4' in str(context.exception))
