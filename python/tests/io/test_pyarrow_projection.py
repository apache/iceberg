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
# pylint: disable=protected-access,unused-argument,redefined-outer-name

from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.avro.resolver import ResolveError
from pyiceberg.expressions import AlwaysTrue, BooleanExpression, GreaterThan
from pyiceberg.io.pyarrow import PyArrowFileIO, project_table, schema_to_pyarrow
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask, Table
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


@pytest.fixture
def schema_int() -> Schema:
    return Schema(NestedField(1, "id", IntegerType(), required=False))


@pytest.fixture
def schema_int_str() -> Schema:
    return Schema(NestedField(1, "id", IntegerType(), required=False), NestedField(2, "data", StringType(), required=False))


@pytest.fixture
def schema_str() -> Schema:
    return Schema(NestedField(2, "data", StringType(), required=False))


@pytest.fixture
def schema_long() -> Schema:
    return Schema(NestedField(3, "id", LongType(), required=False))


@pytest.fixture
def schema_struct() -> Schema:
    return Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType()),
                NestedField(42, "long", DoubleType()),
            ),
        )
    )


@pytest.fixture
def schema_list() -> Schema:
    return Schema(
        NestedField(5, "ids", ListType(51, IntegerType(), element_required=False), required=False),
    )


@pytest.fixture
def schema_list_of_structs() -> Schema:
    return Schema(
        NestedField(
            5,
            "locations",
            ListType(
                51,
                StructType(NestedField(511, "lat", DoubleType()), NestedField(512, "long", DoubleType())),
                element_required=False,
            ),
            required=False,
        ),
    )


@pytest.fixture
def schema_map() -> Schema:
    return Schema(
        NestedField(
            5,
            "properties",
            MapType(
                key_id=51,
                key_type=StringType(),
                value_id=52,
                value_type=StringType(),
                value_required=True,
            ),
            required=False,
        ),
    )


def _write_table_to_file(filepath: str, schema: pa.Schema, table: pa.Table) -> str:
    with pq.ParquetWriter(filepath, schema) as writer:
        writer.write_table(table)
    return filepath


@pytest.fixture
def file_int(schema_int: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_int), metadata={"iceberg.schema": schema_int.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/a.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array([0, 1, 2])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_int_str(schema_int_str: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_int_str), metadata={"iceberg.schema": schema_int_str.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/a.parquet",
        pyarrow_schema,
        pa.Table.from_arrays([pa.array([0, 1, 2]), pa.array(["0", "1", "2"])], schema=pyarrow_schema),
    )


@pytest.fixture
def file_string(schema_str: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_str), metadata={"iceberg.schema": schema_str.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/b.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array(["0", "1", "2"])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_long(schema_long: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_long), metadata={"iceberg.schema": schema_long.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/c.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array([0, 1, 2])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_struct(schema_struct: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_struct), metadata={"iceberg.schema": schema_struct.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/d.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"location": {"lat": 52.371807, "long": 4.896029}},
                {"location": {"lat": 52.387386, "long": 4.646219}},
                {"location": {"lat": 52.078663, "long": 4.288788}},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_list(schema_list: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_list), metadata={"iceberg.schema": schema_list.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"ids": list(range(1, 10))},
                {"ids": list(range(2, 20))},
                {"ids": list(range(3, 30))},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_list_of_structs(schema_list_of_structs: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(
        schema_to_pyarrow(schema_list_of_structs), metadata={"iceberg.schema": schema_list_of_structs.json()}
    )
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"locations": [{"lat": 52.371807, "long": 4.896029}, {"lat": 52.387386, "long": 4.646219}]},
                {"locations": []},
                {"locations": [{"lat": 52.078663, "long": 4.288788}, {"lat": 52.387386, "long": 4.646219}]},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_map(schema_map: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_map), metadata={"iceberg.schema": schema_map.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"properties": [("a", "b")]},
                {"properties": [("c", "d")]},
                {"properties": [("e", "f"), ("g", "h")]},
            ],
            schema=pyarrow_schema,
        ),
    )


def project(
    schema: Schema, files: List[str], expr: Optional[BooleanExpression] = None, table_schema: Optional[Schema] = None
) -> pa.Table:
    return project_table(
        [
            FileScanTask(
                DataFile(file_path=file, file_format=FileFormat.PARQUET, partition={}, record_count=3, file_size_in_bytes=3)
            )
            for file in files
        ],
        Table(
            ("namespace", "table"),
            metadata=TableMetadataV2(
                location="file://a/b/",
                last_column_id=1,
                format_version=2,
                schemas=[table_schema or schema],
                partition_specs=[PartitionSpec()],
            ),
            metadata_location="file://a/b/c.json",
            io=PyArrowFileIO(),
        ),
        expr or AlwaysTrue(),
        schema,
        case_sensitive=True,
    )


def test_projection_add_column(file_int: str) -> None:
    schema = Schema(
        # All new IDs
        NestedField(10, "id", IntegerType(), required=False),
        NestedField(20, "list", ListType(21, IntegerType(), element_required=False), required=False),
        NestedField(
            30,
            "map",
            MapType(key_id=31, key_type=IntegerType(), value_id=32, value_type=StringType(), value_required=False),
            required=False,
        ),
        NestedField(
            40,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False), NestedField(42, "lon", DoubleType(), required=False)
            ),
            required=False,
        ),
    )
    result_table = project(schema, [file_int])

    for col in result_table.columns:
        assert len(col) == 3

    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[1], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[2], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[3], [None, None, None]):
        assert actual.as_py() == expected
    assert (
        repr(result_table.schema)
        == """id: int32
list: list<element: int32>
  child 0, element: int32
    -- field metadata --
    id: '21'
map: map<int32, string>
  child 0, entries: struct<key: int32 not null, value: string> not null
      child 0, key: int32 not null
      -- field metadata --
      id: '31'
      child 1, value: string
      -- field metadata --
      id: '32'
location: struct<lat: double, lon: double>
  child 0, lat: double
    -- field metadata --
    id: '41'
  child 1, lon: double
    -- field metadata --
    id: '42'"""
    )


def test_read_list(schema_list: Schema, file_list: str) -> None:
    result_table = project(schema_list, [file_list])

    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [list(range(1, 10)), list(range(2, 20)), list(range(3, 30))]):
        assert actual.as_py() == expected

    assert repr(result_table.schema) == "ids: list<item: int32>\n  child 0, item: int32"


def test_read_map(schema_map: Schema, file_map: str) -> None:
    result_table = project(schema_map, [file_map])

    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [[("a", "b")], [("c", "d")], [("e", "f"), ("g", "h")]]):
        assert actual.as_py() == expected

    assert (
        repr(result_table.schema)
        == """properties: map<string, string>
  child 0, entries: struct<key: string not null, value: string> not null
      child 0, key: string not null
      child 1, value: string"""
    )


def test_projection_add_column_struct(schema_int: Schema, file_int: str) -> None:
    schema = Schema(
        # A new ID
        NestedField(
            2,
            "id",
            MapType(key_id=3, key_type=IntegerType(), value_id=4, value_type=StringType(), value_required=False),
            required=False,
        )
    )
    result_table = project(schema, [file_int])
    # Everything should be None
    for r in result_table.columns[0]:
        assert r.as_py() is None
    print(result_table.schema)
    assert (
        repr(result_table.schema)
        == """id: map<int32, string>
  child 0, entries: struct<key: int32 not null, value: string> not null
      child 0, key: int32 not null
      -- field metadata --
      id: '3'
      child 1, value: string
      -- field metadata --
      id: '4'"""
    )


def test_projection_add_column_struct_required(file_int: str) -> None:
    schema = Schema(
        # A new ID
        NestedField(
            2,
            "other_id",
            IntegerType(),
            required=True,
        )
    )
    with pytest.raises(ResolveError) as exc_info:
        _ = project(schema, [file_int])
    assert "Field is required, and could not be found in the file: 2: other_id: required int" in str(exc_info.value)


def test_projection_rename_column(schema_int: Schema, file_int: str) -> None:
    schema = Schema(
        # Reuses the id 1
        NestedField(1, "other_name", IntegerType())
    )
    result_table = project(schema, [file_int])
    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [0, 1, 2]):
        assert actual.as_py() == expected

    assert repr(result_table.schema) == "other_name: int32 not null"


def test_projection_concat_files(schema_int: Schema, file_int: str) -> None:
    result_table = project(schema_int, [file_int, file_int])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2, 0, 1, 2]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 6
    assert repr(result_table.schema) == "id: int32"


def test_projection_filter(schema_int: Schema, file_int: str) -> None:
    result_table = project(schema_int, [file_int], GreaterThan("id", 4))
    assert len(result_table.columns[0]) == 0
    assert (
        repr(result_table.schema)
        == """id: int32
  -- field metadata --
  id: '1'"""
    )


def test_projection_filter_renamed_column(file_int: str) -> None:
    schema = Schema(
        # Reuses the id 1
        NestedField(1, "other_id", IntegerType())
    )
    result_table = project(schema, [file_int], GreaterThan("other_id", 1))
    assert len(result_table.columns[0]) == 1
    assert repr(result_table.schema) == "other_id: int32 not null"


def test_projection_filter_add_column(schema_int: Schema, file_int: str, file_string: str) -> None:
    """We have one file that has the column, and the other one doesn't"""
    result_table = project(schema_int, [file_int, file_string])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2, None, None, None]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 6
    assert repr(result_table.schema) == "id: int32"


def test_projection_filter_add_column_promote(file_int: str) -> None:
    schema_long = Schema(NestedField(1, "id", LongType()))
    result_table = project(schema_long, [file_int])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert repr(result_table.schema) == "id: int64 not null"


def test_projection_filter_add_column_demote(file_long: str) -> None:
    schema_int = Schema(NestedField(3, "id", IntegerType()))
    with pytest.raises(ResolveError) as exc_info:
        _ = project(schema_int, [file_long])
    assert "Cannot promote long to int" in str(exc_info.value)


def test_projection_nested_struct_subset(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType()),
                # long is missing!
            ),
        )
    )

    result_table = project(schema, [file_struct])

    for actual, expected in zip(result_table.columns[0], [52.371807, 52.387386, 52.078663]):
        assert actual.as_py() == {"lat": expected}

    assert len(result_table.columns[0]) == 3
    assert repr(result_table.schema) == "location: struct<lat: double not null> not null\n  child 0, lat: double not null"


def test_projection_nested_new_field(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(43, "null", DoubleType(), required=False),  # Whoa, this column doesn't exist in the file
            ),
        )
    )

    result_table = project(schema, [file_struct])

    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == {"null": expected}
    assert len(result_table.columns[0]) == 3
    assert repr(result_table.schema) == "location: struct<null: double> not null\n  child 0, null: double"


def test_projection_nested_struct(schema_struct: Schema, file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False),
                NestedField(43, "null", DoubleType(), required=False),
                NestedField(42, "long", DoubleType(), required=False),
            ),
        )
    )

    result_table = project(schema, [file_struct])
    for actual, expected in zip(
        result_table.columns[0],
        [
            {"lat": 52.371807, "long": 4.896029, "null": None},
            {"lat": 52.387386, "long": 4.646219, "null": None},
            {"lat": 52.078663, "long": 4.288788, "null": None},
        ],
    ):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert (
        repr(result_table.schema)
        == "location: struct<lat: double, null: double, long: double> not null\n  child 0, lat: double\n  child 1, null: double\n  child 2, long: double"
    )


def test_projection_list_of_structs(schema_list_of_structs: Schema, file_list_of_structs: str) -> None:
    schema = Schema(
        NestedField(
            5,
            "locations",
            ListType(
                51,
                StructType(
                    NestedField(511, "latitude", DoubleType()),
                    NestedField(512, "longitude", DoubleType()),
                    NestedField(513, "altitude", DoubleType(), required=False),
                ),
                element_required=False,
            ),
            required=False,
        ),
    )

    result_table = project(schema, [file_list_of_structs])
    assert len(result_table.columns) == 1
    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(
        result_table.columns[0],
        [
            [
                {"latitude": 52.371807, "longitude": 4.896029, "altitude": None},
                {"latitude": 52.387386, "longitude": 4.646219, "altitude": None},
            ],
            [],
            [
                {"latitude": 52.078663, "longitude": 4.288788, "altitude": None},
                {"latitude": 52.387386, "longitude": 4.646219, "altitude": None},
            ],
        ],
    ):
        assert actual.as_py() == expected
    assert (
        repr(result_table.schema)
        == """locations: list<item: struct<latitude: double not null, longitude: double not null, altitude: double>>
  child 0, item: struct<latitude: double not null, longitude: double not null, altitude: double>
      child 0, latitude: double not null
      child 1, longitude: double not null
      child 2, altitude: double"""
    )


def test_projection_nested_struct_different_parent_id(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            5,  # 😱 this is 4 in the file, this will be fixed when projecting the file schema
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False), NestedField(42, "long", DoubleType(), required=False)
            ),
            required=False,
        )
    )

    result_table = project(schema, [file_struct])
    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert (
        repr(result_table.schema)
        == """location: struct<lat: double, long: double>
  child 0, lat: double
    -- field metadata --
    id: '41'
  child 1, long: double
    -- field metadata --
    id: '42'"""
    )


def test_projection_filter_on_unprojected_field(schema_int_str: Schema, file_int_str: str) -> None:
    schema = Schema(NestedField(1, "id", IntegerType()))

    result_table = project(schema, [file_int_str], GreaterThan("data", "1"), schema_int_str)

    for actual, expected in zip(
        result_table.columns[0],
        [2],
    ):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 1
    assert repr(result_table.schema) == "id: int32 not null"


def test_projection_filter_on_unknown_field(schema_int_str: Schema, file_int_str: str) -> None:
    schema = Schema(NestedField(1, "id", IntegerType()))

    with pytest.raises(ValueError) as exc_info:
        _ = project(schema, [file_int_str], GreaterThan("unknown_field", "1"), schema_int_str)

    assert "Could not find field with name unknown_field, case_sensitive=True" in str(exc_info.value)
