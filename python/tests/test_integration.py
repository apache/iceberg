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
# pylint:disable=redefined-outer-name

import math

import pytest

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import IsNaN, NotNaN
from pyiceberg.table import Table


@pytest.fixture()
def catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


@pytest.fixture()
def table_test_null_nan(catalog: Catalog) -> Table:
    return catalog.load_table("default.test_null_nan")


@pytest.fixture()
def table_test_null_nan_rewritten(catalog: Catalog) -> Table:
    return catalog.load_table("default.test_null_nan_rewritten")


@pytest.mark.integration
def test_pyarrow_nan(table_test_null_nan: Table) -> None:
    arrow_table = table_test_null_nan.scan(row_filter=IsNaN("col_numeric"), selected_fields=("idx", "col_numeric")).to_arrow()
    assert len(arrow_table) == 1
    assert arrow_table["idx"][0].as_py() == 1
    assert math.isnan(arrow_table["col_numeric"][0].as_py())


@pytest.mark.integration
def test_pyarrow_nan_rewritten(table_test_null_nan_rewritten: Table) -> None:
    arrow_table = table_test_null_nan_rewritten.scan(
        row_filter=IsNaN("col_numeric"), selected_fields=("idx", "col_numeric")
    ).to_arrow()
    assert len(arrow_table) == 1
    assert arrow_table["idx"][0].as_py() == 1
    assert math.isnan(arrow_table["col_numeric"][0].as_py())


@pytest.mark.integration
@pytest.mark.skip(reason="Fixing issues with NaN's: https://github.com/apache/arrow/issues/34162")
def test_pyarrow_not_nan_count(table_test_null_nan: Table) -> None:
    not_nan = table_test_null_nan.scan(row_filter=NotNaN("col_numeric"), selected_fields=("idx",)).to_arrow()
    assert len(not_nan) == 2


@pytest.mark.integration
def test_duckdb_nan(table_test_null_nan_rewritten: Table) -> None:
    con = table_test_null_nan_rewritten.scan().to_duckdb("table_test_null_nan")
    result = con.query("SELECT idx, col_numeric FROM table_test_null_nan WHERE isnan(col_numeric)").fetchone()
    assert result[0] == 1
    assert math.isnan(result[1])
