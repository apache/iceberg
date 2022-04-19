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

import pytest

from iceberg.expressions import base


@pytest.mark.parametrize(
    "operation,opposite_operation",
    [
        (base.Operation.TRUE, base.Operation.FALSE),
        (base.Operation.FALSE, base.Operation.TRUE),
        (base.Operation.IS_NULL, base.Operation.NOT_NULL),
        (base.Operation.NOT_NULL, base.Operation.IS_NULL),
        (base.Operation.IS_NAN, base.Operation.NOT_NAN),
        (base.Operation.NOT_NAN, base.Operation.IS_NAN),
        (base.Operation.LT, base.Operation.GT_EQ),
        (base.Operation.LT_EQ, base.Operation.GT),
        (base.Operation.GT, base.Operation.LT_EQ),
        (base.Operation.GT_EQ, base.Operation.LT),
        (base.Operation.EQ, base.Operation.NOT_EQ),
        (base.Operation.NOT_EQ, base.Operation.EQ),
        (base.Operation.IN, base.Operation.NOT_IN),
        (base.Operation.NOT_IN, base.Operation.IN),
    ],
)
def test_negation_of_operations(operation, opposite_operation):
    assert operation.negate() == opposite_operation


@pytest.mark.parametrize(
    "operation",
    [
        base.Operation.NOT,
        base.Operation.AND,
        base.Operation.OR,
    ],
)
def test_raise_on_no_negation_for_operation(operation):
    with pytest.raises(ValueError) as exc_info:
        operation.negate()

    assert str(exc_info.value) == f"No negation defined for operation {operation}"
