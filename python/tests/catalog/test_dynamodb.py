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
from typing import List

import pytest
from moto import mock_dynamodb

from pyiceberg.catalog import METADATA_LOCATION, TABLE_TYPE
from pyiceberg.catalog.dynamodb import (
    DYNAMODB_COL_CREATED_AT,
    DYNAMODB_COL_IDENTIFIER,
    DYNAMODB_COL_NAMESPACE,
    DYNAMODB_TABLE_NAME_DEFAULT,
    DynamoDbCatalog,
    _add_property_prefix,
)
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from tests.conftest import BUCKET_NAME, TABLE_METADATA_LOCATION_REGEX


@mock_dynamodb
def test_create_dynamodb_catalog_with_table_name(_dynamodb, _bucket_initialize: None, _patch_aiobotocore: None) -> None:  # type: ignore
    DynamoDbCatalog("test_ddb_catalog")
    response = _dynamodb.describe_table(TableName=DYNAMODB_TABLE_NAME_DEFAULT)
    assert response["Table"]["TableName"] == DYNAMODB_TABLE_NAME_DEFAULT

    custom_table_name = "custom_table_name"
    DynamoDbCatalog("test_ddb_catalog", **{"table-name": custom_table_name})
    response = _dynamodb.describe_table(TableName=custom_table_name)
    assert response["Table"]["TableName"] == custom_table_name


@mock_dynamodb
def test_create_table_with_database_location(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    test_catalog.create_namespace(namespace=database_name, properties={"location": f"s3://{BUCKET_NAME}/{database_name}.db"})
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_dynamodb
def test_create_table_with_default_warehouse(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(
        catalog_name, **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO", "warehouse": f"s3://{BUCKET_NAME}"}
    )
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_dynamodb
def test_create_table_with_given_location(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(
        identifier=identifier, schema=table_schema_nested, location=f"s3://{BUCKET_NAME}/{database_name}.db/{table_name}"
    )
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_dynamodb
def test_create_table_with_no_location(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name)
    with pytest.raises(ValueError):
        test_catalog.create_table(identifier=identifier, schema=table_schema_nested)


@mock_dynamodb
def test_create_table_with_strips(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    test_catalog.create_namespace(namespace=database_name, properties={"location": f"s3://{BUCKET_NAME}/{database_name}.db/"})
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_dynamodb
def test_create_table_with_strips_bucket_root(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(
        catalog_name, **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO", "warehouse": f"s3://{BUCKET_NAME}/"}
    )
    test_catalog.create_namespace(namespace=database_name)
    table_strip = test_catalog.create_table(identifier, table_schema_nested)
    assert table_strip.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table_strip.metadata_location)


@mock_dynamodb
def test_create_table_with_no_database(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier=identifier, schema=table_schema_nested)


@mock_dynamodb
def test_create_duplicated_table(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(
        "test_ddb_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    )
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_table(identifier, table_schema_nested)


@mock_dynamodb
def test_load_table(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(
        catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    )
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_dynamodb
def test_load_non_exist_table(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_dynamodb
def test_drop_table(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(
        catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    )
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.drop_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_dynamodb
def test_drop_non_exist_table(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", warehouse=f"s3://{BUCKET_NAME}")
    with pytest.raises(NoSuchTableError):
        test_catalog.drop_table(identifier)


@mock_dynamodb
def test_rename_table(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (database_name, new_table_name)
    test_catalog = DynamoDbCatalog(
        catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    )
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.rename_table(identifier, new_identifier)
    new_table = test_catalog.load_table(new_identifier)
    assert new_table.identifier == (catalog_name,) + new_identifier
    # the metadata_location should not change
    assert new_table.metadata_location == table.metadata_location
    # old table should be dropped
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_dynamodb
def test_fail_on_rename_table_with_missing_required_params(
    _bucket_initialize: None, _patch_aiobotocore: None, database_name: str, table_name: str
) -> None:
    new_database_name = f"{database_name}_new"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (new_database_name, new_table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_namespace(namespace=new_database_name)

    # Missing required params
    # pylint: disable=W0212
    test_catalog._put_dynamo_item(
        item={
            DYNAMODB_COL_IDENTIFIER: {"S": f"{database_name}.{table_name}"},
            DYNAMODB_COL_NAMESPACE: {"S": database_name},
        },
        condition_expression=f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})",
    )

    with pytest.raises(NoSuchPropertyException):
        test_catalog.rename_table(identifier, new_identifier)


@mock_dynamodb
def test_fail_on_rename_non_iceberg_table(_dynamodb, _bucket_initialize: None, _patch_aiobotocore: None, database_name: str, table_name: str) -> None:  # type: ignore
    new_database_name = f"{database_name}_new"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (new_database_name, new_table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_namespace(namespace=new_database_name)

    # Wrong TABLE_TYPE param
    # pylint: disable=W0212
    test_catalog._put_dynamo_item(
        item={
            DYNAMODB_COL_IDENTIFIER: {"S": f"{database_name}.{table_name}"},
            DYNAMODB_COL_NAMESPACE: {"S": database_name},
            DYNAMODB_COL_CREATED_AT: {"S": "test-1873287263487623"},
            _add_property_prefix(TABLE_TYPE): {"S": "non-iceberg-table-type"},
            _add_property_prefix(METADATA_LOCATION): {"S": "test-metadata-location"},
        },
        condition_expression=f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})",
    )

    with pytest.raises(NoSuchIcebergTableError):
        test_catalog.rename_table(identifier, new_identifier)


@mock_dynamodb
def test_list_tables(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_list: List[str]
) -> None:
    test_catalog = DynamoDbCatalog(
        "test_ddb_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    )
    test_catalog.create_namespace(namespace=database_name)
    for table_name in table_list:
        test_catalog.create_table((database_name, table_name), table_schema_nested)
    loaded_table_list = test_catalog.list_tables(database_name)
    for table_name in table_list:
        assert (database_name, table_name) in loaded_table_list


@mock_dynamodb
def test_list_namespaces(_bucket_initialize: None, _patch_aiobotocore: None, database_list: List[str]) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    for database_name in database_list:
        test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    for database_name in database_list:
        assert (database_name,) in loaded_database_list


@mock_dynamodb
def test_create_namespace_no_properties(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties == {}


@mock_dynamodb
def test_create_namespace_with_comment_and_location(
    _bucket_initialize: None, _patch_aiobotocore: None, database_name: str
) -> None:
    test_location = f"s3://{BUCKET_NAME}/{database_name}.db"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name, properties=test_properties)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


@mock_dynamodb
def test_create_duplicated_namespace(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    with pytest.raises(NamespaceAlreadyExistsError):
        test_catalog.create_namespace(namespace=database_name, properties={"test": "test"})


@mock_dynamodb
def test_drop_namespace(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    test_catalog.drop_namespace(database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 0


@mock_dynamodb
def test_drop_non_empty_namespace(
    _bucket_initialize: None, _patch_aiobotocore: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(
        "test_ddb_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    )
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    assert len(test_catalog.list_tables(database_name)) == 1
    with pytest.raises(NamespaceNotEmptyError):
        test_catalog.drop_namespace(database_name)


@mock_dynamodb
def test_drop_non_exist_namespace(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.drop_namespace(database_name)


@mock_dynamodb
def test_load_namespace_properties(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str) -> None:
    test_location = f"s3://{BUCKET_NAME}/{database_name}.db"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(database_name, test_properties)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


@mock_dynamodb
def test_load_non_exist_namespace_properties(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.load_namespace_properties(database_name)


@mock_dynamodb
def test_update_namespace_properties(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str) -> None:
    test_properties = {
        "comment": "this is a test description",
        "location": f"s3://{BUCKET_NAME}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(database_name, test_properties)
    update_report = test_catalog.update_namespace_properties(database_name, removals, updates)
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert "updated test description" == test_catalog.load_namespace_properties(database_name)["comment"]
    test_catalog.drop_namespace(database_name)


@mock_dynamodb
def test_load_empty_namespace_properties(_bucket_initialize: None, _patch_aiobotocore: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(database_name)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    assert listed_properties == {}


@mock_dynamodb
def test_update_namespace_properties_overlap_update_removal(
    _bucket_initialize: None, _patch_aiobotocore: None, database_name: str
) -> None:
    test_properties = {
        "comment": "this is a test description",
        "location": f"s3://{BUCKET_NAME}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property1": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(database_name, test_properties)
    with pytest.raises(ValueError):
        test_catalog.update_namespace_properties(database_name, removals, updates)
    # should not modify the properties
    assert test_catalog.load_namespace_properties(database_name) == test_properties
