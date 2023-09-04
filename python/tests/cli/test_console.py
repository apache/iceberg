#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import os

import pytest
from click.testing import CliRunner
from pytest_mock import MockFixture

from pyiceberg.cli.console import run
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Properties
from pyiceberg.types import LongType, NestedField
from pyiceberg.utils.config import Config
from tests.catalog.test_base import InMemoryCatalog


def test_missing_uri(mocker: MockFixture, empty_home_dir_path: str) -> None:
    # mock to prevent parsing ~/.pyiceberg.yaml or {PYICEBERG_HOME}/.pyiceberg.yaml
    mocker.patch.dict(os.environ, values={"HOME": empty_home_dir_path, "PYICEBERG_HOME": empty_home_dir_path})
    mocker.patch("pyiceberg.catalog._ENV_CONFIG", return_value=Config())

    runner = CliRunner()
    result = runner.invoke(run, ["list"])

    assert result.exit_code == 1
    assert result.output == "Could not initialize catalog with the following properties: {}\n"


@pytest.fixture(autouse=True)
def env_vars(mocker: MockFixture) -> None:
    mocker.patch.dict(os.environ, MOCK_ENVIRONMENT)


@pytest.fixture(name="catalog")
def fixture_catalog(mocker: MockFixture) -> InMemoryCatalog:
    in_memory_catalog = InMemoryCatalog("test.in.memory.catalog", **{"test.key": "test.value"})
    mocker.patch("pyiceberg.cli.console.load_catalog", return_value=in_memory_catalog)
    return in_memory_catalog


@pytest.fixture(name="namespace_properties")
def fixture_namespace_properties() -> Properties:
    return TEST_NAMESPACE_PROPERTIES.copy()


TEST_TABLE_IDENTIFIER = ("default", "my_table")
TEST_TABLE_NAMESPACE = "default"
TEST_NAMESPACE_PROPERTIES = {"location": "s3://warehouse/database/location"}
TEST_TABLE_NAME = "my_table"
TEST_TABLE_SCHEMA = Schema(
    NestedField(1, "x", LongType()),
    NestedField(2, "y", LongType(), doc="comment"),
    NestedField(3, "z", LongType()),
)
TEST_TABLE_LOCATION = "s3://bucket/test/location"
TEST_TABLE_PARTITION_SPEC = PartitionSpec(PartitionField(name="x", transform=IdentityTransform(), source_id=1, field_id=1000))
TEST_TABLE_PROPERTIES = {"read.split.target.size": "134217728"}
MOCK_ENVIRONMENT = {"PYICEBERG_CATALOG__PRODUCTION__URI": "test://doesnotexist"}


def test_list_root(catalog: InMemoryCatalog) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)

    runner = CliRunner()
    result = runner.invoke(run, ["list"])

    assert result.exit_code == 0
    assert TEST_TABLE_NAMESPACE in result.output


def test_list_namespace(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["list", "default"])

    assert result.exit_code == 0
    assert result.output == "default.my_table\n"


def test_describe_namespace(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default"])

    assert result.exit_code == 0
    assert result.output == "location  s3://warehouse/database/location\n"


def test_describe_namespace_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["describe", "doesnotexist"])

    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: ('doesnotexist',)\n"


def test_describe_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default.my_table"])
    assert result.exit_code == 0
    assert (
        # Strip the whitespace on the end
        "\n".join([line.rstrip() for line in result.output.split("\n")])
        == """Table format version  1
Metadata location     s3://warehouse/default/my_table/metadata/metadata.json
Table UUID            d20125c8-7284-442c-9aea-15fee620737c
Last Updated          1602638573874
Partition spec        [
                        1000: x: identity(1)
                      ]
Sort order            []
Current schema        Schema, id=0
                      ├── 1: x: required long
                      ├── 2: y: required long (comment)
                      └── 3: z: required long
Current snapshot      None
Snapshots             Snapshots
                      └── Snapshot 1925, schema None
Properties
"""
    )


def test_describe_table_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table or namespace does not exist: default.doesnotexist\n"


def test_schema(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["schema", "default.my_table"])
    assert result.exit_code == 0
    assert (
        "\n".join([line.rstrip() for line in result.output.split("\n")])
        == """x  long
y  long  comment
z  long
"""
    )


def test_schema_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["schema", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('default', 'doesnotexist')\n"


def test_spec(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["spec", "default.my_table"])
    assert result.exit_code == 0
    assert (
        result.output
        == """[
  1000: x: identity(1)
]
"""
    )


def test_spec_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["spec", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('default', 'doesnotexist')\n"


def test_uuid(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["uuid", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == """d20125c8-7284-442c-9aea-15fee620737c\n"""


def test_uuid_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["uuid", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('default', 'doesnotexist')\n"


def test_location(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["location", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == """s3://bucket/test/location\n"""


def test_location_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["location", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('default', 'doesnotexist')\n"


def test_drop_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["drop", "table", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == """Dropped table: default.my_table\n"""


def test_drop_table_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["drop", "table", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('default', 'doesnotexist')\n"


def test_drop_namespace(catalog: InMemoryCatalog) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)

    runner = CliRunner()
    result = runner.invoke(run, ["drop", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == """Dropped namespace: default\n"""


def test_drop_namespace_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["drop", "namespace", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: ('doesnotexist',)\n"


def test_rename_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["rename", "default.my_table", "default.my_new_table"])
    assert result.exit_code == 0
    assert result.output == """Renamed table from default.my_table to default.my_new_table\n"""


def test_rename_table_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["rename", "default.doesnotexist", "default.bar"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('default', 'doesnotexist')\n"


def test_properties_get_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "table", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == "read.split.target.size  134217728\n"


def test_properties_get_table_specific_property(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "table", "default.my_table", "read.split.target.size"])
    assert result.exit_code == 0
    assert result.output == "134217728\n"


def test_properties_get_table_specific_property_that_doesnt_exist(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "table", "default.my_table", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Could not find property doesnotexist on table default.my_table\n"


def test_properties_get_table_does_not_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "table", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('doesnotexist',)\n"


def test_properties_get_namespace(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == "location  s3://warehouse/database/location\n"


def test_properties_get_namespace_specific_property(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == "s3://warehouse/database/location\n"


def test_properties_get_namespace_does_not_exist(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "namespace", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: ('doesnotexist',)\n"


def test_properties_set_namespace(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "namespace", "default", "location", "s3://new_location"])
    assert result.exit_code == 0
    assert result.output == "Updated location on default\n"


def test_properties_set_namespace_that_doesnt_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "namespace", "doesnotexist", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: ('doesnotexist',)\n"


def test_properties_set_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "table", "default.my_table", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


def test_properties_set_table_does_not_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "table", "default.doesnotexist", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('default', 'doesnotexist')\n"


def test_properties_remove_namespace(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == "Property location removed from default\n"


def test_properties_remove_namespace_that_doesnt_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "namespace", "doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: ('doesnotexist',)\n"


def test_properties_remove_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.my_table", "read.split.target.size"])
    assert result.exit_code == 1
    assert result.output == "Writing is WIP\n1\n"


def test_properties_remove_table_property_does_not_exists(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.my_table", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Property doesnotexist does not exist on default.my_table\n"


def test_properties_remove_table_does_not_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: ('default', 'doesnotexist')\n"


def test_json_list_root(catalog: InMemoryCatalog) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "list"])
    assert result.exit_code == 0
    assert result.output == """["default"]\n"""


def test_json_list_namespace(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "list", "default"])
    assert result.exit_code == 0
    assert result.output == """["default.my_table"]\n"""


def test_json_describe_namespace(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default"])
    assert result.exit_code == 0
    assert result.output == """{"location": "s3://warehouse/database/location"}\n"""


def test_json_describe_namespace_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: ('doesnotexist',)"}\n"""


def test_json_describe_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default.my_table"])
    assert result.exit_code == 0
    assert (
        result.output
        == """{"identifier":["default","my_table"],"metadata_location":"s3://warehouse/default/my_table/metadata/metadata.json","metadata":{"location":"s3://bucket/test/location","table-uuid":"d20125c8-7284-442c-9aea-15fee620737c","last-updated-ms":1602638573874,"last-column-id":3,"schemas":[{"type":"struct","fields":[{"id":1,"name":"x","type":"long","required":true},{"id":2,"name":"y","type":"long","required":true,"doc":"comment"},{"id":3,"name":"z","type":"long","required":true}],"schema-id":0,"identifier-field-ids":[]}],"current-schema-id":0,"partition-specs":[{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,"transform":"identity","name":"x"}]}],"default-spec-id":0,"last-partition-id":1000,"properties":{},"snapshots":[{"snapshot-id":1925,"timestamp-ms":1602638573822}],"snapshot-log":[],"metadata-log":[],"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"refs":{},"format-version":1,"schema":{"type":"struct","fields":[{"id":1,"name":"x","type":"long","required":true},{"id":2,"name":"y","type":"long","required":true,"doc":"comment"},{"id":3,"name":"z","type":"long","required":true}],"schema-id":0,"identifier-field-ids":[]},"partition-spec":[{"source-id":1,"field-id":1000,"transform":"identity","name":"x"}]}}\n"""
    )


def test_json_describe_table_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default.doesnotexist"])
    assert result.exit_code == 1
    assert (
        result.output
        == """{"type": "NoSuchTableError", "message": "Table or namespace does not exist: default.doesnotexist"}\n"""
    )


def test_json_schema(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "schema", "default.my_table"])
    assert result.exit_code == 0
    assert (
        result.output
        == """{"type":"struct","fields":[{"id":1,"name":"x","type":"long","required":true},{"id":2,"name":"y","type":"long","required":true,"doc":"comment"},{"id":3,"name":"z","type":"long","required":true}],"schema-id":0,"identifier-field-ids":[]}\n"""
    )


def test_json_schema_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "schema", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('default', 'doesnotexist')"}\n"""


def test_json_spec(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "spec", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == """{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,"transform":"identity","name":"x"}]}\n"""


def test_json_spec_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "spec", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('default', 'doesnotexist')"}\n"""


def test_json_uuid(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "uuid", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == """{"uuid": "d20125c8-7284-442c-9aea-15fee620737c"}\n"""


def test_json_uuid_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "uuid", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('default', 'doesnotexist')"}\n"""


def test_json_location(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "location", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == """"s3://bucket/test/location"\n"""


def test_json_location_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "location", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('default', 'doesnotexist')"}\n"""


def test_json_drop_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "table", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == """"Dropped table: default.my_table"\n"""


def test_json_drop_table_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "table", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('default', 'doesnotexist')"}\n"""


def test_json_drop_namespace(catalog: InMemoryCatalog) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == """"Dropped namespace: default"\n"""


def test_json_drop_namespace_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "namespace", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: ('doesnotexist',)"}\n"""


def test_json_rename_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "rename", "default.my_table", "default.my_new_table"])
    assert result.exit_code == 0
    assert result.output == """"Renamed table from default.my_table to default.my_new_table"\n"""


def test_json_rename_table_does_not_exists(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "rename", "default.doesnotexist", "default.bar"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('default', 'doesnotexist')"}\n"""


def test_json_properties_get_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "table", "default.my_table"])
    assert result.exit_code == 0
    assert result.output == """{"read.split.target.size": "134217728"}\n"""


def test_json_properties_get_table_specific_property(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "table", "default.my_table", "read.split.target.size"])
    assert result.exit_code == 0
    assert result.output == """"134217728"\n"""


def test_json_properties_get_table_specific_property_that_doesnt_exist(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "table", "default.my_table", "doesnotexist"])
    assert result.exit_code == 1
    assert (
        result.output
        == """{"type": "NoSuchPropertyException", "message": "Could not find property doesnotexist on table default.my_table"}\n"""
    )


def test_json_properties_get_table_does_not_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "table", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('doesnotexist',)"}\n"""


def test_json_properties_get_namespace(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == """{"location": "s3://warehouse/database/location"}\n"""


def test_json_properties_get_namespace_specific_property(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == """"s3://warehouse/database/location"\n"""


def test_json_properties_get_namespace_does_not_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "namespace", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: ('doesnotexist',)"}\n"""


def test_json_properties_set_namespace(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "set", "namespace", "default", "location", "s3://new_location"])
    assert result.exit_code == 0
    assert result.output == """"Updated location on default"\n"""


def test_json_properties_set_namespace_that_doesnt_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(
        run, ["--output=json", "properties", "set", "namespace", "doesnotexist", "location", "s3://new_location"]
    )
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: ('doesnotexist',)"}\n"""


def test_json_properties_set_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(
        run, ["--output=json", "properties", "set", "table", "default.my_table", "location", "s3://new_location"]
    )
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


def test_json_properties_set_table_does_not_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(
        run, ["--output=json", "properties", "set", "table", "default.doesnotexist", "location", "s3://new_location"]
    )
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('default', 'doesnotexist')"}\n"""


def test_json_properties_remove_namespace(catalog: InMemoryCatalog, namespace_properties: Properties) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE, namespace_properties)

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == """"Property location removed from default"\n"""


def test_json_properties_remove_namespace_that_doesnt_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "namespace", "doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: ('doesnotexist',)"}\n"""


def test_json_properties_remove_table(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.my_table", "read.split.target.size"])
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


def test_json_properties_remove_table_property_does_not_exists(catalog: InMemoryCatalog) -> None:
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.my_table", "doesnotexist"])
    assert result.exit_code == 1
    assert (
        result.output
        == """{"type": "NoSuchPropertyException", "message": "Property doesnotexist does not exist on default.my_table"}\n"""
    )


def test_json_properties_remove_table_does_not_exist(catalog: InMemoryCatalog) -> None:
    # pylint: disable=unused-argument

    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: ('default', 'doesnotexist')"}\n"""
