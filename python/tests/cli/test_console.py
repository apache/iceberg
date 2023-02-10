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
from typing import (
    List,
    Optional,
    Set,
    Union,
)
from unittest import mock

from click.testing import CliRunner

from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
from pyiceberg.cli.console import run
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from pyiceberg.utils.config import Config

EXAMPLE_TABLE_METADATA_V2 = {
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 34,
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
    "current-schema-id": 1,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]},
        {
            "type": "struct",
            "schema-id": 1,
            "identifier-field-ids": [1, 2],
            "fields": [
                {"id": 1, "name": "x", "required": True, "type": "long"},
                {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
                {"id": 3, "name": "z", "required": True, "type": "long"},
            ],
        },
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
    "last-partition-id": 1000,
    "default-sort-order-id": 3,
    "sort-orders": [
        {
            "order-id": 3,
            "fields": [
                {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"},
            ],
        }
    ],
    "properties": {"read.split.target.size": 134217728},
    "current-snapshot-id": 3055729675574597004,
    "snapshots": [
        {
            "snapshot-id": 3051729675574597004,
            "timestamp-ms": 1515100955770,
            "sequence-number": 0,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/1.avro",
        },
        {
            "snapshot-id": 3055729675574597004,
            "parent-snapshot-id": 3051729675574597004,
            "timestamp-ms": 1555100955770,
            "sequence-number": 1,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/2.avro",
            "schema-id": 1,
        },
    ],
    "snapshot-log": [
        {"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770},
        {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770},
    ],
    "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}],
    "refs": {"test": {"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}},
}


class MockCatalog(Catalog):
    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        return Table(
            identifier=Catalog.identifier_to_tuple(identifier),
            metadata_location="s3://tmp/",
            metadata=TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2),
            io=load_file_io(),
        )

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        tuple_identifier = Catalog.identifier_to_tuple(identifier)
        if tuple_identifier == ("default", "foo"):
            return Table(
                identifier=tuple_identifier,
                metadata_location="s3://tmp/",
                metadata=TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2),
                io=load_file_io(),
            )
        else:
            raise NoSuchTableError(f"Table does not exist: {'.'.join(tuple_identifier)}")

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        tuple_identifier = Catalog.identifier_to_tuple(identifier)
        if tuple_identifier == ("default", "foo"):
            return None
        else:
            raise NoSuchTableError(f"Table does not exist: {identifier}")

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self.drop_table(identifier)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        tuple_identifier = Catalog.identifier_to_tuple(from_identifier)
        if tuple_identifier == ("default", "foo"):
            return Table(
                identifier=tuple_identifier,
                metadata_location="s3://tmp/",
                metadata=TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2),
                io=load_file_io(),
            )
        else:
            raise NoSuchTableError(f"Table does not exist: {from_identifier}")

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        return None

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        tuple_identifier = Catalog.identifier_to_tuple(namespace)
        if tuple_identifier == ("default",):
            return None
        else:
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        return [
            ("default", "foo"),
            ("default", "bar"),
        ]

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        # No hierarchical namespaces for now
        if namespace == ():
            return [("default",), ("personal",)]
        else:
            return []

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        identifier = Catalog.identifier_to_tuple(namespace)
        if identifier == ("default",):
            return {"location": "s3://warehouse/database/location"}
        else:
            raise NoSuchNamespaceError(f"Namespace does not exist: {'.'.join(namespace)}")

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        identifier = Catalog.identifier_to_tuple(namespace)

        if identifier == ("default",):
            return PropertiesUpdateSummary(removed=["location"], updated=[], missing=[])
        else:
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")


MOCK_CATALOG = MockCatalog("production", uri="http://somewhere")
MOCK_ENVIRONMENT = {"PYICEBERG_CATALOG__PRODUCTION__URI": "test://doesnotexist"}


def test_missing_uri(empty_home_dir_path: str) -> None:
    # mock to prevent parsing ~/.pyiceberg.yaml or {PYICEBERG_HOME}/.pyiceberg.yaml
    with mock.patch.dict(os.environ, {"HOME": empty_home_dir_path, "PYICEBERG_HOME": empty_home_dir_path}):
        with mock.patch("pyiceberg.catalog._ENV_CONFIG", Config()):
            runner = CliRunner()
            result = runner.invoke(run, ["list"])
            assert result.exit_code == 1
            assert (
                result.output
                == "URI missing, please provide using --uri, the config or environment variable \nPYICEBERG_CATALOG__DEFAULT__URI\n"
            )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_list_root(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["list"])
    assert result.exit_code == 0
    assert result.output == "default \npersonal\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_list_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["list", "default"])
    assert result.exit_code == 0
    assert result.output == "default.foo\ndefault.bar\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_describe_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default"])
    assert result.exit_code == 0
    assert result.output == "location  s3://warehouse/database/location\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_describe_namespace_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["describe", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_describe_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default.foo"])
    assert result.exit_code == 0
    assert (
        # Strip the whitespace on the end
        "\n".join([line.rstrip() for line in result.output.split("\n")])
        == """Table format version  2
Metadata location     s3://tmp/
Table UUID            9c12d441-03fe-4693-9a96-a0705ddf69c1
Last Updated          1602638573590
Partition spec        [
                        1000: x: identity(1)
                      ]
Sort order            [
                        2 ASC NULLS FIRST
                        bucket[4](3) DESC NULLS LAST
                      ]
Current schema        Schema, id=1
                      ├── 1: x: required long
                      ├── 2: y: required long (comment)
                      └── 3: z: required long
Current snapshot      Operation.APPEND: id=3055729675574597004,
                      parent_id=3051729675574597004, schema_id=1
Snapshots             Snapshots
                      ├── Snapshot 3051729675574597004, schema None:
                      │   s3://a/b/1.avro
                      └── Snapshot 3055729675574597004, schema 1:
                          s3://a/b/2.avro
Properties            read.split.target.size  134217728
"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_describe_table_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table or namespace does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_schema(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["schema", "default.foo"])
    assert result.exit_code == 0
    assert (
        "\n".join([line.rstrip() for line in result.output.split("\n")])
        == """x  long
y  long  comment
z  long
"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_schema_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["schema", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_spec(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["spec", "default.foo"])
    assert result.exit_code == 0
    assert (
        result.output
        == """[
  1000: x: identity(1)
]
"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_spec_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["spec", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_uuid(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["uuid", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """9c12d441-03fe-4693-9a96-a0705ddf69c1\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_uuid_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["uuid", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_location(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["location", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """s3://bucket/test/location\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_location_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["location", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_drop_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["drop", "table", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """Dropped table: default.foo\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_drop_table_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["drop", "table", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_drop_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["drop", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == """Dropped namespace: default\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_drop_namespace_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["drop", "namespace", "doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_rename_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["rename", "default.foo", "default.bar"])
    assert result.exit_code == 0
    assert result.output == """Renamed table from default.foo to default.bar\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_rename_table_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["rename", "default.doesnotexit", "default.bar"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_get_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "table", "default.foo"])
    assert result.exit_code == 0
    assert result.output == "read.split.target.size  134217728\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_get_table_specific_property(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "table", "default.foo", "read.split.target.size"])
    assert result.exit_code == 0
    assert result.output == "134217728\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_get_table_specific_property_that_doesnt_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "table", "default.foo", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Could not find property doesnotexist on table default.foo\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_get_table_does_not_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "table", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_get_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == "location  s3://warehouse/database/location\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_get_namespace_specific_property(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == "s3://warehouse/database/location\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_get_namespace_does_not_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "namespace", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_set_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "namespace", "default", "location", "s3://new_location"])
    assert result.exit_code == 0
    assert result.output == "Updated location on default\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_set_namespace_that_doesnt_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "namespace", "doesnotexist", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_set_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "table", "default.foo", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_set_table_does_not_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "table", "default.doesnotexist", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_remove_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == "Property location removed from default\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_remove_namespace_that_doesnt_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "namespace", "doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_remove_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.foo", "read.split.target.size"])
    assert result.exit_code == 1
    assert result.output == "Writing is WIP\n1\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_remove_table_property_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.foo", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Property doesnotexist does not exist on default.foo\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_properties_remove_table_does_not_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_list_root(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "list"])
    assert result.exit_code == 0
    assert result.output == """["default", "personal"]\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_list_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "list", "default"])
    assert result.exit_code == 0
    assert result.output == """["default.foo", "default.bar"]\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_describe_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default"])
    assert result.exit_code == 0
    assert result.output == """{"location": "s3://warehouse/database/location"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_describe_namespace_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_describe_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default.foo"])
    assert result.exit_code == 0
    assert (
        result.output
        == """{"identifier": ["default", "foo"], "metadata_location": "s3://tmp/", "metadata": {"location": "s3://bucket/test/location", "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "last-updated-ms": 1602638573590, "last-column-id": 3, "schemas": [{"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}, {"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 1, "identifier-field-ids": [1, 2]}], "current-schema-id": 1, "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "x"}]}], "default-spec-id": 0, "last-partition-id": 1000, "properties": {"read.split.target.size": "134217728"}, "current-snapshot-id": 3055729675574597004, "snapshots": [{"snapshot-id": 3051729675574597004, "sequence-number": 0, "timestamp-ms": 1515100955770, "manifest-list": "s3://a/b/1.avro", "summary": {"operation": "append"}}, {"snapshot-id": 3055729675574597004, "parent-snapshot-id": 3051729675574597004, "sequence-number": 1, "timestamp-ms": 1555100955770, "manifest-list": "s3://a/b/2.avro", "summary": {"operation": "append"}, "schema-id": 1}], "snapshot-log": [{"snapshot-id": "3051729675574597004", "timestamp-ms": 1515100955770}, {"snapshot-id": "3055729675574597004", "timestamp-ms": 1555100955770}], "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}], "sort-orders": [{"order-id": 3, "fields": [{"source-id": 2, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}, {"source-id": 3, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"}]}], "default-sort-order-id": 3, "refs": {"test": {"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}, "main": {"snapshot-id": 3055729675574597004, "type": "branch"}}, "format-version": 2, "last-sequence-number": 34}}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_describe_table_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default.doesnotexit"])
    assert result.exit_code == 1
    assert (
        result.output == """{"type": "NoSuchTableError", "message": "Table or namespace does not exist: default.doesnotexit"}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_schema(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "schema", "default.foo"])
    assert result.exit_code == 0
    assert (
        result.output
        == """{"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 1, "identifier-field-ids": [1, 2]}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_schema_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "schema", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_spec(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "spec", "default.foo"])
    assert result.exit_code == 0
    assert (
        result.output
        == """{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "x"}]}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_spec_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "spec", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_uuid(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "uuid", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """{"uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_uuid_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "uuid", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_location(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "location", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """"s3://bucket/test/location"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_location_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "location", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_drop_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "table", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """"Dropped table: default.foo"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_drop_table_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "table", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_drop_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == """"Dropped namespace: default"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_drop_namespace_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "namespace", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_rename_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "rename", "default.foo", "default.bar"])
    assert result.exit_code == 0
    assert result.output == """"Renamed table from default.foo to default.bar"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_rename_table_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "rename", "default.doesnotexit", "default.bar"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_get_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "table", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """{"read.split.target.size": "134217728"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_get_table_specific_property(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "table", "default.foo", "read.split.target.size"])
    assert result.exit_code == 0
    assert result.output == """"134217728"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_get_table_specific_property_that_doesnt_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "table", "default.foo", "doesnotexist"])
    assert result.exit_code == 1
    assert (
        result.output
        == """{"type": "NoSuchPropertyException", "message": "Could not find property doesnotexist on table default.foo"}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_get_table_does_not_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "table", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_get_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == """{"location": "s3://warehouse/database/location"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_get_namespace_specific_property(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == """"s3://warehouse/database/location"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_get_namespace_does_not_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "namespace", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_set_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "set", "namespace", "default", "location", "s3://new_location"])
    assert result.exit_code == 0
    assert result.output == """"Updated location on default"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_set_namespace_that_doesnt_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(
        run, ["--output=json", "properties", "set", "namespace", "doesnotexist", "location", "s3://new_location"]
    )
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_set_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "set", "table", "default.foo", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_set_table_does_not_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(
        run, ["--output=json", "properties", "set", "table", "default.doesnotexist", "location", "s3://new_location"]
    )
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_remove_namespace(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == """"Property location removed from default"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_remove_namespace_that_doesnt_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "namespace", "doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_remove_table(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.foo", "read.split.target.size"])
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_remove_table_property_does_not_exists(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.foo", "doesnotexist"])
    assert result.exit_code == 1
    assert (
        result.output
        == """{"type": "NoSuchPropertyException", "message": "Property doesnotexist does not exist on default.foo"}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.load_catalog", return_value=MOCK_CATALOG)
def test_json_properties_remove_table_does_not_exist(_: MockCatalog) -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexist"}\n"""
