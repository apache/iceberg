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

from iceberg.api.expressions import Expressions
from iceberg.parquet.dataset_utils import get_dataset_filter
import pyarrow.dataset as ds
import pytest


@pytest.mark.parametrize("expr, dataset_filter, column_map",
                         [(Expressions.greater_than('a', 1), ds.field('a') > 1, {'a': 'a'}),
                          (Expressions.greater_than_or_equal('a', 1), ds.field('a') >= 1, {'a': 'a'}),
                          (Expressions.less_than('a', 1), ds.field('a') < 1, {'a': 'a'}),
                          (Expressions.less_than_or_equal('a', 1), ds.field('a') <= 1, {'a': 'a'}),
                          (Expressions.equal('a', 1), ds.field('a') == 1, {'a': 'a'}),
                          (Expressions.not_equal('a', 1), ds.field('a') != 1, {'a': 'a'}),
                          (Expressions.not_null('a'), ds.field('a').is_valid(), {'a': 'a'}),
                          (Expressions.is_null('a'), ~ds.field('a').is_valid(), {'a': 'a'})

                          ])
def test_simple(expr, dataset_filter, column_map):
    translated_dataset_filter = get_dataset_filter(expr, column_map)
    assert dataset_filter.equals(translated_dataset_filter)


def test_not_conversion():
    expr = Expressions.not_(Expressions.greater_than('a', 1))
    translated_dataset_filter = get_dataset_filter(expr, {'a': 'a'})
    assert (~(ds.field("a") > 1)).equals(translated_dataset_filter)


def test_complex_expr():
    expr = Expressions.or_(Expressions.and_(Expressions.greater_than('a', 1), Expressions.equal("b", "US")),
                           Expressions.equal("c", True))

    translated_dataset_filter = get_dataset_filter(expr, {'a': 'a', 'b': 'b', 'c': 'c'})
    dataset_filter = (((ds.field("a") > 1) & (ds.field("b") == "US")) | (ds.field("c") == True))  # noqa: E712
    assert dataset_filter.equals(translated_dataset_filter)
