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

import pyarrow as pa
import pytest


@pytest.fixture(scope="session")
def pyarrow_array():
    return [pa.array([1, 2, 3, None, 5], type=pa.int32()),
            pa.array(['us', 'can', 'us', 'us', 'can'], type=pa.string()),
            pa.array([[0], [1, 2], [1], [1, 2, 3], None], type=pa.list_(pa.int64())),
            pa.array([True, None, False, True, True], pa.bool_())]


@pytest.fixture(scope="session")
def pytable_colnames():
    return ['int_col', 'str_col', 'list_col', 'bool_col']
