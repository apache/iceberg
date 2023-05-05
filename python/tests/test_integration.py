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
from urllib.parse import urlparse

import pyarrow.parquet as pq
import pytest
from pyarrow.fs import S3FileSystem

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import IsNaN, NotNaN
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.schema import Schema
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


@pytest.fixture()
def table_test_limit(catalog: Catalog) -> Table:
    return catalog.load_table("default.test_limit")


@pytest.fixture()
def table_test_all_types(catalog: Catalog) -> Table:
    return catalog.load_table("default.test_all_types")


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


@pytest.mark.integration
def test_pyarrow_limit(table_test_limit: Table) -> None:
    limited_result = table_test_limit.scan(selected_fields=("idx",), limit=1).to_arrow()
    assert len(limited_result) == 1

    empty_result = table_test_limit.scan(selected_fields=("idx",), limit=0).to_arrow()
    assert len(empty_result) == 0

    full_result = table_test_limit.scan(selected_fields=("idx",), limit=999).to_arrow()
    assert len(full_result) == 10


@pytest.mark.integration
def test_ray_nan(table_test_null_nan_rewritten: Table) -> None:
    ray_dataset = table_test_null_nan_rewritten.scan().to_ray()
    assert ray_dataset.count() == 3
    assert math.isnan(ray_dataset.take()[0]["col_numeric"])


@pytest.mark.integration
def test_ray_nan_rewritten(table_test_null_nan_rewritten: Table) -> None:
    ray_dataset = table_test_null_nan_rewritten.scan(
        row_filter=IsNaN("col_numeric"), selected_fields=("idx", "col_numeric")
    ).to_ray()
    assert ray_dataset.count() == 1
    assert ray_dataset.take()[0]["idx"] == 1
    assert math.isnan(ray_dataset.take()[0]["col_numeric"])


@pytest.mark.integration
@pytest.mark.skip(reason="Fixing issues with NaN's: https://github.com/apache/arrow/issues/34162")
def test_ray_not_nan_count(table_test_null_nan_rewritten: Table) -> None:
    ray_dataset = table_test_null_nan_rewritten.scan(row_filter=NotNaN("col_numeric"), selected_fields=("idx",)).to_ray()
    print(ray_dataset.take())
    assert ray_dataset.count() == 2


@pytest.mark.integration
def test_ray_all_types(table_test_all_types: Table) -> None:
    ray_dataset = table_test_all_types.scan().to_ray()
    pandas_dataframe = table_test_all_types.scan().to_pandas()
    assert ray_dataset.count() == pandas_dataframe.shape[0]
    assert pandas_dataframe.equals(ray_dataset.to_pandas())


@pytest.mark.integration
def test_pyarrow_to_iceberg_all_types(table_test_all_types: Table) -> None:
    fs = S3FileSystem(
        **{
            "endpoint_override": "http://localhost:9000",
            "access_key": "admin",
            "secret_key": "password",
        }
    )
    data_file_paths = [task.file.file_path for task in table_test_all_types.scan().plan_files()]
    for data_file_path in data_file_paths:
        uri = urlparse(data_file_path)
        with fs.open_input_file(f"{uri.netloc}{uri.path}") as fout:
            parquet_schema = pq.read_schema(fout)
            stored_iceberg_schema = Schema.parse_raw(parquet_schema.metadata.get(b"iceberg.schema"))
            converted_iceberg_schema = pyarrow_to_schema(parquet_schema)
            assert converted_iceberg_schema == stored_iceberg_schema
