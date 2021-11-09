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

from iceberg import expressions


@pytest.mark.parametrize(
    "operation,opposite_operation",
    [
        (expressions.Operation.TRUE, expressions.Operation.FALSE),
        (expressions.Operation.FALSE, expressions.Operation.TRUE),
        (expressions.Operation.IS_NULL, expressions.Operation.NOT_NULL),
        (expressions.Operation.NOT_NULL, expressions.Operation.IS_NULL),
        (expressions.Operation.IS_NAN, expressions.Operation.NOT_NAN),
        (expressions.Operation.NOT_NAN, expressions.Operation.IS_NAN),
        (expressions.Operation.LT, expressions.Operation.GT_EQ),
        (expressions.Operation.LT_EQ, expressions.Operation.GT),
        (expressions.Operation.GT, expressions.Operation.LT_EQ),
        (expressions.Operation.GT_EQ, expressions.Operation.LT),
        (expressions.Operation.EQ, expressions.Operation.NOT_EQ),
        (expressions.Operation.NOT_EQ, expressions.Operation.EQ),
        (expressions.Operation.IN, expressions.Operation.NOT_IN),
        (expressions.Operation.NOT_IN, expressions.Operation.IN),
    ],
)
def test_negation_of_operations(operation, opposite_operation):
    assert operation.negate() == opposite_operation


@pytest.mark.parametrize(
    "operation",
    [
        expressions.Operation.NOT,
        expressions.Operation.AND,
        expressions.Operation.OR,
    ],
)
def test_raise_on_no_negation_for_operation(operation):
    with pytest.raises(ValueError) as exc_info:
        operation.negate()

    assert str(exc_info.value) == f"No negation defined for operation {operation}"
