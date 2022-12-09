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
            "uri": f"http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


@pytest.fixture()
def table_beers(catalog: Catalog) -> Table:
    return catalog.load_table("default.beers")


@pytest.mark.integration
def test_pyarrow_nan(table_beers: Table) -> None:
    """To check if we detect NaN values properly"""
    arrow_table = table_beers.scan(row_filter=IsNaN("ibu"), selected_fields=("beer_id", "ibu")).to_arrow()
    assert arrow_table[0][0].as_py() == 2546
    assert math.isnan(arrow_table[1][0].as_py())


@pytest.mark.integration
def test_pyarrow_not_nan_count(table_beers: Table) -> None:
    """To check if exclude NaN values properly"""
    not_nan = table_beers.scan(row_filter=NotNaN("ibu"), selected_fields=("beer_id", "ibu")).to_arrow()
    total = table_beers.scan(selected_fields=("beer_id", "ibu")).to_arrow()
    assert len(total) - 1 == len(not_nan)


@pytest.mark.integration
@pytest.mark.skip(reason="Seems to be a bug in the PyArrow to DuckDB conversion")
def test_duckdb_nan(table_beers: Table) -> None:
    """To check if we detect NaN values properly"""
    con = table_beers.scan(row_filter=IsNaN("ibu"), selected_fields=("beer_id", "ibu")).to_duckdb("beers")
    assert con.query("SELECT beer_id, ibu FROM beers WHERE ibu = 'NaN'").fetchone() == (2546, float("NaN"))
