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

from collections import OrderedDict

from iceberg.api.expressions import Expressions
from iceberg.parquet.gandiva_utils import apply_iceberg_filter
import pyarrow as pa
import pytest


@pytest.mark.parametrize("filter_value, col_name, op, ref_dict",
                         [(1, "int_col", Expressions.equal, OrderedDict([('int_col', [1]),
                                                                         ('str_col', ['us']),
                                                                         ('list_col', [[0]]),
                                                                         ('bool_col', [True])])),
                          (3, "int_col", Expressions.less_than_or_equal, OrderedDict([('int_col', [1, 2, 3]),
                                                                                      ('str_col', ['us', 'can', 'us']),
                                                                                      ('list_col', [[0], [1, 2], [1]]),
                                                                                      ('bool_col', [True, None, False])])),
                          (3, "int_col", Expressions.less_than, OrderedDict([('int_col', [1, 2]),
                                                                             ('str_col', ['us', 'can']),
                                                                             ('list_col', [[0], [1, 2]]),
                                                                             ('bool_col', [True, None])])),
                          (5, "int_col", Expressions.greater_than_or_equal, OrderedDict([('int_col', [5]),
                                                                                         ('str_col', ['can']),
                                                                                         ('list_col', [None]),
                                                                                         ('bool_col', [True])])),
                          (2, "int_col", Expressions.greater_than, OrderedDict([('int_col', [3, 5]),
                                                                                ('str_col', ['us', 'can']),
                                                                                ('list_col', [[1], None]),
                                                                                ('bool_col', [False, True])]))
                          ])
def test_int_comparison_expressions(pytable_colnames, pyarrow_array, filter_value, col_name, op, ref_dict):
    batches = [pa.RecordBatch.from_arrays(pyarrow_array, pytable_colnames)]
    tbl = pa.Table.from_batches(batches)

    filt_table = apply_iceberg_filter(tbl, {col: col for col in pytable_colnames}, op(col_name, filter_value))

    assert filt_table.to_batches()[0].to_pydict() == ref_dict


@pytest.mark.parametrize("filter_value, col_name, op, ref_dict",
                         [("us", "str_col", Expressions.equal, OrderedDict([('int_col', [1, 3, None]),
                                                                            ('str_col', ['us', 'us', 'us']),
                                                                            ('list_col', [[0], [1], [1, 2, 3]]),
                                                                            ('bool_col', [True, False, True])])),
                          ("can", "str_col", Expressions.less_than_or_equal, OrderedDict([('int_col', [2, 5]),
                                                                                          ('str_col', ['can', 'can']),
                                                                                          ('list_col', [[1, 2], None]),
                                                                                          ('bool_col', [None, True])])),
                          ("cb", "str_col", Expressions.less_than, OrderedDict([('int_col', [2, 5]),
                                                                                ('str_col', ['can', 'can']),
                                                                                ('list_col', [[1, 2], None]),
                                                                                ('bool_col', [None, True])])),
                          ("us", "str_col", Expressions.greater_than_or_equal, OrderedDict([('int_col', [1, 3, None]),
                                                                                            ('str_col', ['us', 'us', 'us']),
                                                                                            ('list_col', [[0], [1], [1, 2, 3]]),
                                                                                            ('bool_col', [True, False, True])])),
                          ("ua", "str_col", Expressions.greater_than, OrderedDict([('int_col', [1, 3, None]),
                                                                                   ('str_col', ['us', 'us', 'us']),
                                                                                   ('list_col', [[0], [1], [1, 2, 3]]),
                                                                                   ('bool_col', [True, False, True])]))
                          ])
def test_str_comparison_expressions(pytable_colnames, pyarrow_array, filter_value, col_name, op, ref_dict):
    batches = [pa.RecordBatch.from_arrays(pyarrow_array, pytable_colnames)]
    tbl = pa.Table.from_batches(batches)

    filt_table = apply_iceberg_filter(tbl, {col: col for col in pytable_colnames}, op(col_name, filter_value))

    assert filt_table.to_batches()[0].to_pydict() == ref_dict


@pytest.mark.parametrize("filter_value, col_name, op, ref_dict",
                         [(True, "bool_col", Expressions.equal, OrderedDict([('int_col', [1, None, 5]),
                                                                             ('str_col', ['us', 'us', 'can']),
                                                                             ('list_col', [[0], [1, 2, 3], None]),
                                                                             ('bool_col', [True, True, True])])),
                          (True, "bool_col", Expressions.not_equal, OrderedDict([('int_col', [3]),
                                                                                 ('str_col', ['us']),
                                                                                 ('list_col', [[1]]),
                                                                                 ('bool_col', [False])])),
                          (False, "bool_col", Expressions.equal, OrderedDict([('int_col', [3]),
                                                                              ('str_col', ['us']),
                                                                              ('list_col', [[1]]),
                                                                              ('bool_col', [False])])),
                          (False, "bool_col", Expressions.not_equal, OrderedDict([('int_col', [1, None, 5]),
                                                                                  ('str_col', ['us', 'us', 'can']),
                                                                                  ('list_col', [[0], [1, 2, 3], None]),
                                                                                  ('bool_col', [True, True, True])]))
                          ])
def test_bool_comparison_expressions(pytable_colnames, pyarrow_array, filter_value, col_name, op, ref_dict):

    batches = [pa.RecordBatch.from_arrays(pyarrow_array, pytable_colnames)]
    tbl = pa.Table.from_batches(batches)

    filt_table = apply_iceberg_filter(tbl, {col: col for col in pytable_colnames}, op(col_name, filter_value))

    assert filt_table.to_batches()[0].to_pydict() == ref_dict


@pytest.mark.parametrize("col_name, op, ref_dict",
                         [("int_col", Expressions.is_null, OrderedDict([('int_col', [None]),
                                                                        ('str_col', ['us']),
                                                                        ('list_col', [[1, 2, 3]]),
                                                                        ('bool_col', [True])])),
                          ("list_col", Expressions.is_null, OrderedDict([('int_col', [5]),
                                                                         ('str_col', ['can']),
                                                                         ('list_col', [None]),
                                                                         ('bool_col', [True])])),
                          ("bool_col", Expressions.is_null, OrderedDict([('int_col', [2]),
                                                                         ('str_col', ['can']),
                                                                         ('list_col', [[1, 2]]),
                                                                         ('bool_col', [None])])),
                          ("int_col", Expressions.not_null, OrderedDict([('int_col', [1, 2, 3, 5]),
                                                                         ('str_col', ['us', 'can', 'us', 'can']),
                                                                         ('list_col', [[0], [1, 2], [1], None]),
                                                                         ('bool_col', [True, None, False, True])])),
                          ("str_col", Expressions.not_null, OrderedDict([('int_col', [1, 2, 3, None, 5]),
                                                                         ('str_col', ['us', 'can', 'us', 'us', 'can']),
                                                                         ('list_col', [[0], [1, 2], [1], [1, 2, 3], None]),
                                                                         ('bool_col', [True, None, False, True, True])])),
                          ("list_col", Expressions.not_null, OrderedDict([('int_col', [1, 2, 3, None]),
                                                                          ('str_col', ['us', 'can', 'us', 'us']),
                                                                          ('list_col', [[0], [1, 2], [1], [1, 2, 3]]),
                                                                          ('bool_col', [True, None, False, True])])),
                          ("bool_col", Expressions.not_null, OrderedDict([('int_col', [1, 3, None, 5]),
                                                                          ('str_col', ['us', 'us', 'us', 'can']),
                                                                          ('list_col', [[0], [1], [1, 2, 3], None]),
                                                                          ('bool_col', [True, False, True, True])])),

                          ])
def test_nullable_expressions(pytable_colnames, pyarrow_array, col_name, op, ref_dict):

    batches = [pa.RecordBatch.from_arrays(pyarrow_array, pytable_colnames)]
    tbl = pa.Table.from_batches(batches)

    with pytest.raises(NotImplementedError):
        apply_iceberg_filter(tbl, {col: col for col in pytable_colnames}, op(col_name))
