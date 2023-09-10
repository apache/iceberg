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
import uuid
from urllib.parse import urlparse

import pyarrow.parquet as pq
import pytest
from pyarrow.fs import S3FileSystem

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThanOrEqual,
    IsNaN,
    LessThan,
    NotEqualTo,
    NotNaN,
)
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)


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


TABLE_NAME = ("default", "t1")


@pytest.fixture()
def table(catalog: Catalog) -> Table:
    try:
        catalog.drop_table(TABLE_NAME)
    except NoSuchTableError:
        pass  # Just to make sure that the table doesn't exist

    schema = Schema(
        NestedField(field_id=1, name="str", field_type=StringType(), required=False),
        NestedField(field_id=2, name="int", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="bool", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="datetime", field_type=TimestampType(), required=False),
        schema_id=1,
    )

    return catalog.create_table(identifier=TABLE_NAME, schema=schema)


@pytest.mark.integration
def test_table_properties(table: Table) -> None:
    assert table.properties == {}

    with table.transaction() as transaction:
        transaction.set_properties(abc="🤪")

    assert table.properties == {"abc": "🤪"}

    with table.transaction() as transaction:
        transaction.remove_properties("abc")

    assert table.properties == {}

    table = table.transaction().set_properties(abc="def").commit_transaction()

    assert table.properties == {"abc": "def"}

    table = table.transaction().remove_properties("abc").commit_transaction()

    assert table.properties == {}


@pytest.fixture()
def test_positional_mor_deletes(catalog: Catalog) -> Table:
    """Table that has positional deletes"""
    return catalog.load_table("default.test_positional_mor_deletes")


@pytest.fixture()
def test_positional_mor_double_deletes(catalog: Catalog) -> Table:
    """Table that has multiple positional deletes"""
    return catalog.load_table("default.test_positional_mor_double_deletes")


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
            stored_iceberg_schema = Schema.model_validate_json(parquet_schema.metadata.get(b"iceberg.schema"))
            converted_iceberg_schema = pyarrow_to_schema(parquet_schema)
            assert converted_iceberg_schema == stored_iceberg_schema


@pytest.mark.integration
def test_pyarrow_deletes(test_positional_mor_deletes: Table) -> None:
    # number, letter
    #  (1, 'a'),
    #  (2, 'b'),
    #  (3, 'c'),
    #  (4, 'd'),
    #  (5, 'e'),
    #  (6, 'f'),
    #  (7, 'g'),
    #  (8, 'h'),
    #  (9, 'i'), <- deleted
    #  (10, 'j'),
    #  (11, 'k'),
    #  (12, 'l')
    arrow_table = test_positional_mor_deletes.scan().to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]

    # Checking the filter
    arrow_table = test_positional_mor_deletes.scan(
        row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k"))
    ).to_arrow()
    assert arrow_table["number"].to_pylist() == [5, 6, 7, 8, 10]

    # Testing the combination of a filter and a limit
    arrow_table = test_positional_mor_deletes.scan(
        row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k")), limit=1
    ).to_arrow()
    assert arrow_table["number"].to_pylist() == [5]

    # Testing the slicing of indices
    arrow_table = test_positional_mor_deletes.scan(limit=3).to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3]


@pytest.mark.integration
def test_pyarrow_deletes_double(test_positional_mor_double_deletes: Table) -> None:
    # number, letter
    #  (1, 'a'),
    #  (2, 'b'),
    #  (3, 'c'),
    #  (4, 'd'),
    #  (5, 'e'),
    #  (6, 'f'), <- second delete
    #  (7, 'g'),
    #  (8, 'h'),
    #  (9, 'i'), <- first delete
    #  (10, 'j'),
    #  (11, 'k'),
    #  (12, 'l')
    arrow_table = test_positional_mor_double_deletes.scan().to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 7, 8, 10, 11, 12]

    # Checking the filter
    arrow_table = test_positional_mor_double_deletes.scan(
        row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k"))
    ).to_arrow()
    assert arrow_table["number"].to_pylist() == [5, 7, 8, 10]

    # Testing the combination of a filter and a limit
    arrow_table = test_positional_mor_double_deletes.scan(
        row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k")), limit=1
    ).to_arrow()
    assert arrow_table["number"].to_pylist() == [5]

    # Testing the slicing of indices
    arrow_table = test_positional_mor_double_deletes.scan(limit=8).to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 7, 8, 10]


@pytest.mark.integration
def test_partitioned_tables(catalog: Catalog) -> None:
    for table_name, predicate in [
        ("test_partitioned_by_identity", "ts >= '2023-03-05T00:00:00+00:00'"),
        ("test_partitioned_by_years", "dt >= '2023-03-05'"),
        ("test_partitioned_by_months", "dt >= '2023-03-05'"),
        ("test_partitioned_by_days", "ts >= '2023-03-05T00:00:00+00:00'"),
        ("test_partitioned_by_hours", "ts >= '2023-03-05T00:00:00+00:00'"),
        ("test_partitioned_by_truncate", "letter >= 'e'"),
        ("test_partitioned_by_bucket", "number >= '5'"),
    ]:
        table = catalog.load_table(f"default.{table_name}")
        arrow_table = table.scan(selected_fields=("number",), row_filter=predicate).to_arrow()
        assert set(arrow_table["number"].to_pylist()) == {5, 6, 7, 8, 9, 10, 11, 12}, f"Table {table_name}, predicate {predicate}"


@pytest.mark.integration
def test_unpartitioned_uuid_table(catalog: Catalog) -> None:
    unpartitioned_uuid = catalog.load_table("default.test_uuid_and_fixed_unpartitioned")
    arrow_table_eq = unpartitioned_uuid.scan(row_filter="uuid_col == '102cb62f-e6f8-4eb0-9973-d9b012ff0967'").to_arrow()
    assert arrow_table_eq["uuid_col"].to_pylist() == [uuid.UUID("102cb62f-e6f8-4eb0-9973-d9b012ff0967").bytes]

    arrow_table_neq = unpartitioned_uuid.scan(
        row_filter="uuid_col != '102cb62f-e6f8-4eb0-9973-d9b012ff0967' and uuid_col != '639cccce-c9d2-494a-a78c-278ab234f024'"
    ).to_arrow()
    assert arrow_table_neq["uuid_col"].to_pylist() == [
        uuid.UUID("ec33e4b2-a834-4cc3-8c4a-a1d3bfc2f226").bytes,
        uuid.UUID("c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b").bytes,
        uuid.UUID("923dae77-83d6-47cd-b4b0-d383e64ee57e").bytes,
    ]


@pytest.mark.integration
def test_unpartitioned_fixed_table(catalog: Catalog) -> None:
    fixed_table = catalog.load_table("default.test_uuid_and_fixed_unpartitioned")
    arrow_table_eq = fixed_table.scan(row_filter=EqualTo("fixed_col", b"1234567890123456789012345")).to_arrow()
    assert arrow_table_eq["fixed_col"].to_pylist() == [b"1234567890123456789012345"]

    arrow_table_neq = fixed_table.scan(
        row_filter=And(
            NotEqualTo("fixed_col", b"1234567890123456789012345"), NotEqualTo("uuid_col", "c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b")
        )
    ).to_arrow()
    assert arrow_table_neq["fixed_col"].to_pylist() == [
        b"1231231231231231231231231",
        b"12345678901234567ass12345",
        b"qweeqwwqq1231231231231111",
    ]


@pytest.mark.integration
def test_scan_tag(test_positional_mor_deletes: Table) -> None:
    arrow_table = test_positional_mor_deletes.scan().use_ref("tag_12").to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


@pytest.mark.integration
def test_scan_branch(test_positional_mor_deletes: Table) -> None:
    arrow_table = test_positional_mor_deletes.scan().use_ref("without_5").to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12]
