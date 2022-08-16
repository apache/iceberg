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
from pyiceberg.schema import Schema
from pyiceberg.table import Table, TableMetadataV2
from pyiceberg.table.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from tests.conftest import EXAMPLE_TABLE_METADATA_V2


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
        return Table(identifier=identifier, metadata_location="s3://tmp/", metadata=TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2))

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        tuple_identifier = Catalog.identifier_to_tuple(identifier)
        if tuple_identifier == ("default", "foo"):
            return Table(
                identifier=tuple_identifier,
                metadata_location="s3://tmp/",
                metadata=TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2),
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
                identifier=tuple_identifier, metadata_location="s3://tmp/", metadata=TableMetadataV2(**EXAMPLE_TABLE_METADATA_V2)
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
        return [("default",), ("personal",)]

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        identifier = Catalog.identifier_to_tuple(namespace)
        if identifier == ("default",):
            return {"location": "s3://warehouse/database/location"}
        else:
            raise NoSuchNamespaceError(f"Namespace does not exists: {namespace}")

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        identifier = Catalog.identifier_to_tuple(namespace)

        if identifier == ("default",):
            return PropertiesUpdateSummary(removed=["location"], updated=[], missing=[])
        else:
            raise NoSuchNamespaceError(f"Namespace does not exists: {namespace}")


MOCK_ENVIRONMENT = {"PYICEBERG_URI": "test://doesnotexist"}
MOCK_CATALOGS = {"test": MockCatalog}


def test_missing_uri():
    runner = CliRunner()
    result = runner.invoke(run, ["list"])
    assert result.exit_code == 1
    assert result.output == "Missing uri. Please provide using --uri or using environment variable \nPYICEBERG_URI\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_list_root():
    runner = CliRunner()
    result = runner.invoke(run, ["list"])
    assert result.exit_code == 0
    assert result.output == "default \npersonal\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_list_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["list", "default"])
    assert result.exit_code == 0
    assert result.output == "default.foo\ndefault.bar\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_describe_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default"])
    assert result.exit_code == 0
    assert result.output == "location  s3://warehouse/database/location\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_describe_namespace_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["describe", "doesnotexists"])
    assert result.exit_code == 1
    assert result.output == "Table or namespace does not exist: doesnotexists\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_describe_table():
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
Schema                Schema
                      ├── 1: x: required long
                      ├── 2: y: required long (comment)
                      └── 3: z: required long
Snapshots             Snapshots
                      ├── Snapshot None: s3://a/b/1.avro
                      └── Snapshot 1: s3://a/b/2.avro
Properties            read.split.target.size  134217728
"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_describe_table_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_schema():
    runner = CliRunner()
    result = runner.invoke(run, ["schema", "default.foo"])
    assert result.exit_code == 0
    assert (
        result.output
        == """x  long
y  long
z  long
"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_schema_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["describe", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_spec():
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
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_spec_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["spec", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_uuid():
    runner = CliRunner()
    result = runner.invoke(run, ["uuid", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """9c12d441-03fe-4693-9a96-a0705ddf69c1\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_uuid_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["uuid", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_location():
    runner = CliRunner()
    result = runner.invoke(run, ["location", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """s3://bucket/test/location\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_location_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["location", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_drop_table():
    runner = CliRunner()
    result = runner.invoke(run, ["drop", "table", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """Dropped table: default.foo\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_drop_table_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["drop", "table", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_drop_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["drop", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == """Dropped namespace: default\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_drop_namespace_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["drop", "namespace", "doesnotexit"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exist: doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_rename_table():
    runner = CliRunner()
    result = runner.invoke(run, ["rename", "default.foo", "default.bar"])
    assert result.exit_code == 0
    assert result.output == """Renamed table from default.foo to default.bar\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_rename_table_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["rename", "default.doesnotexit", "default.bar"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexit\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_get_table():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "default.foo"])
    assert result.exit_code == 0
    assert result.output == "read.split.target.size  134217728\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_get_table_specific_property():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "default.foo", "read.split.target.size"])
    assert result.exit_code == 0
    assert result.output == "134217728\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_get_table_specific_property_that_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "default.fokko", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table or namespace does not exist: default.fokko with property doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_get_table_does_not_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Table or namespace does not exist: doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_get_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "default"])
    assert result.exit_code == 0
    assert result.output == "location  s3://warehouse/database/location\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_get_namespace_specific_property():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "get", "default", "location"])
    assert result.exit_code == 0
    assert result.output == "s3://warehouse/database/location\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_set_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "namespace", "default", "location", "s3://new_location"])
    assert result.exit_code == 0
    assert result.output == "Updated location on default\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_set_namespace_that_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "namespace", "doesnotexists", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exists: doesnotexists\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_set_table():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "table", "default.foo", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_set_table_does_not_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "set", "table", "default.doesnotexist", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_remove_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == "Property location removed from default\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_remove_namespace_that_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "namespace", "doesnotexists", "location"])
    assert result.exit_code == 1
    assert result.output == "Namespace does not exists: doesnotexists\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_remove_table():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.foo", "read.split.target.size"])
    assert result.exit_code == 1
    assert result.output == "Writing is WIP\n1\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_remove_table_property_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.foo", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == "Property doesnotexist does not exists on default.foo\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_properties_remove_table_does_not_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["properties", "remove", "table", "default.doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == "Table does not exist: default.doesnotexist\n"


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_list_root():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "list"])
    assert result.exit_code == 0
    assert result.output == """["default", "personal"]\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_list_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "list", "default"])
    assert result.exit_code == 0
    assert result.output == """["default.foo", "default.bar"]\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_describe_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default"])
    assert result.exit_code == 0
    assert result.output == """{"location": "s3://warehouse/database/location"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_describe_namespace_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "doesnotexists"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table or namespace does not exist: doesnotexists"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_describe_table():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default.foo"])
    assert result.exit_code == 0
    assert (
        result.output
        == """{"identifier": ["default", "foo"], "metadata_location": "s3://tmp/", "metadata": {"location": "s3://bucket/test/location", "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "last-updated-ms": 1602638573590, "last-column-id": 3, "schemas": [{"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}, {"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 1, "identifier-field-ids": [1, 2]}], "current-schema-id": 1, "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "x"}]}], "default-spec-id": 0, "last-partition-id": 1000, "properties": {"read.split.target.size": "134217728"}, "current-snapshot-id": 3055729675574597004, "snapshots": [{"snapshot-id": 3051729675574597004, "sequence-number": 0, "timestamp-ms": 1515100955770, "manifest-list": "s3://a/b/1.avro", "summary": {"operation": "append"}}, {"snapshot-id": 3055729675574597004, "parent-snapshot-id": 3051729675574597004, "sequence-number": 1, "timestamp-ms": 1555100955770, "manifest-list": "s3://a/b/2.avro", "summary": {"operation": "append"}, "schema-id": 1}], "snapshot-log": [{"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770}, {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}], "metadata-log": [], "sort-orders": [{"order-id": 3, "fields": [{"source-id": 2, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}, {"source-id": 3, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"}]}], "default-sort-order-id": 3, "refs": {"main": {"snapshot-id": 3055729675574597004, "type": "branch"}}, "format-version": 2, "last-sequence-number": 34}}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_describe_table_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_schema():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "schema", "default.foo"])
    assert result.exit_code == 0
    assert (
        result.output
        == """{"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 1, "identifier-field-ids": [1, 2]}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_schema_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "describe", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_spec():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "spec", "default.foo"])
    assert result.exit_code == 0
    assert (
        result.output
        == """{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "x"}]}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_spec_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "spec", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_uuid():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "uuid", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """{"uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_uuid_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "uuid", "default.doesnotexit"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_location():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "location", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """"s3://bucket/test/location"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_location_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "location", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_drop_table():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "table", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """"Dropped table: default.foo"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_drop_table_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "table", "default.doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_drop_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "namespace", "default"])
    assert result.exit_code == 0
    assert result.output == """"Dropped namespace: default"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_drop_namespace_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "drop", "namespace", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exist: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_rename_table():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "rename", "default.foo", "default.bar"])
    assert result.exit_code == 0
    assert result.output == """"Renamed table from default.foo to default.bar"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_rename_table_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "rename", "default.doesnotexit", "default.bar"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexit"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_get_table():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "default.foo"])
    assert result.exit_code == 0
    assert result.output == """{"read.split.target.size": "134217728"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_get_table_specific_property():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "default.foo", "read.split.target.size"])
    assert result.exit_code == 0
    assert result.output == """"134217728"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_get_table_specific_property_that_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "default.fokko", "doesnotexist"])
    assert result.exit_code == 1
    assert (
        result.output
        == """{"type": "NoSuchNamespaceError", "message": "Table or namespace does not exist: default.fokko with property doesnotexist"}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_get_table_does_not_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "doesnotexist"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Table or namespace does not exist: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_get_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "default"])
    assert result.exit_code == 0
    assert result.output == """{"location": "s3://warehouse/database/location"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_get_namespace_specific_property():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "get", "default", "location"])
    assert result.exit_code == 0
    assert result.output == """"s3://warehouse/database/location"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_set_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "set", "namespace", "default", "location", "s3://new_location"])
    assert result.exit_code == 0
    assert result.output == """"Updated location on default"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_set_namespace_that_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(
        run, ["--output=json", "properties", "set", "namespace", "doesnotexists", "location", "s3://new_location"]
    )
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exists: doesnotexists"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_set_table():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "set", "table", "default.foo", "location", "s3://new_location"])
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_set_table_does_not_exist():
    runner = CliRunner()
    result = runner.invoke(
        run, ["--output=json", "properties", "set", "table", "default.doesnotexist", "location", "s3://new_location"]
    )
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_remove_namespace():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "namespace", "default", "location"])
    assert result.exit_code == 0
    assert result.output == """"Property location removed from default"\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_remove_namespace_that_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "namespace", "doesnotexist", "location"])
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchNamespaceError", "message": "Namespace does not exists: doesnotexist"}\n"""


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_remove_table():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.foo", "read.split.target.size"])
    assert result.exit_code == 1
    assert "Writing is WIP" in result.output


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_remove_table_property_does_not_exists():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.foo", "doesnotexist"])
    assert result.exit_code == 1
    assert (
        result.output
        == """{"type": "NoSuchPropertyException", "message": "Property doesnotexist does not exists on default.foo"}\n"""
    )


@mock.patch.dict(os.environ, MOCK_ENVIRONMENT)
@mock.patch("pyiceberg.cli.console.SUPPORTED_CATALOGS", MOCK_CATALOGS)
def test_json_properties_remove_table_does_not_exist():
    runner = CliRunner()
    result = runner.invoke(run, ["--output=json", "properties", "remove", "table", "default.doesnotexist", "location"])
    print(result)
    assert result.exit_code == 1
    assert result.output == """{"type": "NoSuchTableError", "message": "Table does not exist: default.doesnotexist"}\n"""
