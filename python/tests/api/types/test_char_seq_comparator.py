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

from iceberg.api.expressions import (Literal,
                                     StringLiteral)
import pytest


@pytest.mark.parametrize("input_vals", [
    (Literal.of("abc"), Literal.of(u'abc')),  # unicode and str are same
    (StringLiteral(None), StringLiteral(None))  # None literals are equal
])
def test_special_equality(input_vals):
    assert input_vals[0] == input_vals[1]


@pytest.mark.parametrize("input_vals", [
    (Literal.of("abc"), Literal.of('abcd')),  # test_seq_length, longer is greater
    (Literal.of('abcd'), Literal.of("adc")),  # test_char_order, first difference takes precedence over length
    (None, Literal.of('abc'))  # test_null_handling, null comes before non-null
])
@pytest.mark.parametrize("eval_func", [
    lambda x, y: y > x,
    lambda x, y: x < y])
def test_seq_length(input_vals, eval_func):
    if input_vals[0] is not None:
        assert eval_func(input_vals[0].value, input_vals[1].value)

    assert eval_func(input_vals[0], input_vals[1])
