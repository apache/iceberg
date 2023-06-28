from typing import Generator, List

import boto3
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.jdbc import JDBCCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NamespaceNotEmptyError, NoSuchNamespaceError, NoSuchTableError
from pyiceberg.schema import Schema
from tests.conftest import clean_up, get_bucket_name, get_s3_path
import os


@pytest.fixture(name="test_catalog", scope="module")
def fixture_test_catalog() -> Generator[JDBCCatalog, None, None]:
    """The pre- and post-setting of JDBC integration test."""
    os.environ["AWS_TEST_BUCKET"] = "warehouse"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

    props = {
        "uri": "postgresql://pguser:pgpass@localhost:5432/",
        "warehouse": get_s3_path(get_bucket_name()),
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
    test_catalog = JDBCCatalog("jdbc_catalog", **props)
    test_catalog.initialize_catalog_tables()
    yield test_catalog
    clean_up(test_catalog)


@pytest.mark.integration
def test_create_table(
    test_catalog: Catalog, s3: boto3.client, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    test_catalog.create_table(identifier, table_schema_nested, get_s3_path(get_bucket_name(), database_name, table_name))
    table = test_catalog.load_table(identifier)
    assert table.identifier == (test_catalog.name,) + identifier
    metadata_location = table.metadata_location.split(get_bucket_name())[1][1:]
    s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)


# def test_create_table_with_invalid_location(table_schema_nested: Schema, database_name: str, table_name: str) -> None:
#     identifier = (database_name, table_name)
#     test_catalog_no_warehouse = DynamoDbCatalog("test_ddb_catalog")
#     test_catalog_no_warehouse.create_namespace(database_name)
#     with pytest.raises(ValueError):
#         test_catalog_no_warehouse.create_table(identifier, table_schema_nested)
#     test_catalog_no_warehouse.drop_namespace(database_name)


# def test_create_table_with_default_location(
#     test_catalog: Catalog, s3: boto3.client, table_schema_nested: Schema, database_name: str, table_name: str
# ) -> None:
#     identifier = (database_name, table_name)
#     test_catalog.create_namespace(database_name)
#     test_catalog.create_table(identifier, table_schema_nested)
#     table = test_catalog.load_table(identifier)
#     assert table.identifier == (test_catalog.name,) + identifier
#     metadata_location = table.metadata_location.split(get_bucket_name())[1][1:]
#     s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)


@pytest.mark.integration
def test_create_table_with_invalid_database(test_catalog: Catalog, table_schema_nested: Schema, table_name: str) -> None:
    identifier = ("invalid", table_name)
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier, table_schema_nested)


# def test_create_duplicated_table(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str) -> None:
#     test_catalog.create_namespace(database_name)
#     test_catalog.create_table((database_name, table_name), table_schema_nested)
#     with pytest.raises(TableAlreadyExistsError):
#         test_catalog.create_table((database_name, table_name), table_schema_nested)


# def test_load_table(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str) -> None:
#     identifier = (database_name, table_name)
#     test_catalog.create_namespace(database_name)
#     table = test_catalog.create_table(identifier, table_schema_nested)
#     loaded_table = test_catalog.load_table(identifier)
#     assert table.identifier == loaded_table.identifier
#     assert table.metadata_location == loaded_table.metadata_location
#     assert table.metadata == loaded_table.metadata


# def test_list_tables(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_list: List[str]) -> None:
#     test_catalog.create_namespace(database_name)
#     for table_name in table_list:
#         test_catalog.create_table((database_name, table_name), table_schema_nested)
#     identifier_list = test_catalog.list_tables(database_name)
#     assert len(identifier_list) == LIST_TEST_NUMBER
#     for table_name in table_list:
#         assert (database_name, table_name) in identifier_list


# def test_rename_table(
#     test_catalog: Catalog, s3: boto3.client, table_schema_nested: Schema, table_name: str, database_name: str
# ) -> None:
#     new_database_name = f"{database_name}_new"
#     test_catalog.create_namespace(database_name)
#     test_catalog.create_namespace(new_database_name)
#     new_table_name = f"rename-{table_name}"
#     identifier = (database_name, table_name)
#     table = test_catalog.create_table(identifier, table_schema_nested)
#     assert table.identifier == (test_catalog.name,) + identifier
#     new_identifier = (new_database_name, new_table_name)
#     test_catalog.rename_table(identifier, new_identifier)
#     new_table = test_catalog.load_table(new_identifier)
#     assert new_table.identifier == (test_catalog.name,) + new_identifier
#     assert new_table.metadata_location == table.metadata_location
#     metadata_location = new_table.metadata_location.split(get_bucket_name())[1][1:]
#     s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)
#     with pytest.raises(NoSuchTableError):
#         test_catalog.load_table(identifier)


# @pytest.mark.integration
# def test_drop_table(test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str) -> None:
#     identifier = (database_name, table_name)
#     test_catalog.create_namespace(database_name)
#     table = test_catalog.create_table(identifier, table_schema_nested)
#     assert table.identifier == (test_catalog.name,) + identifier
#     test_catalog.drop_table(identifier)
#     with pytest.raises(NoSuchTableError):
#         test_catalog.load_table(identifier)


# def test_purge_table(
#     test_catalog: Catalog, s3: boto3.client, table_schema_nested: Schema, table_name: str, database_name: str
# ) -> None:
#     identifier = (database_name, table_name)
#     test_catalog.create_namespace(database_name)
#     test_catalog.create_table(identifier, table_schema_nested)
#     table = test_catalog.load_table(identifier)
#     assert table.identifier == (test_catalog.name,) + identifier
#     metadata_location = table.metadata_location.split(get_bucket_name())[1][1:]
#     s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)
#     test_catalog.purge_table(identifier)
#     with pytest.raises(NoSuchTableError):
#         test_catalog.load_table(identifier)
#     with pytest.raises(ClientError):
#         s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)


@pytest.mark.integration
def test_create_namespace(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()


@pytest.mark.integration
def test_create_duplicate_namespace(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    with pytest.raises(NamespaceAlreadyExistsError):
        test_catalog.create_namespace(database_name)


@pytest.mark.integration
def test_create_namespace_with_comment_and_location(test_catalog: Catalog, database_name: str) -> None:
    # TODO: Change location to be arbitrary string, this test should not have a dependency or anything to do with S3
    # test_location = get_s3_path(get_bucket_name(), database_name)
    test_location = "/a/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    test_catalog.create_namespace(namespace=database_name, properties=test_properties)
    loaded_database_list = test_catalog.list_namespaces()
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


@pytest.mark.integration
def test_list_namespaces(test_catalog: Catalog, database_list: List[str]) -> None:
    for database_name in database_list:
        test_catalog.create_namespace(database_name)
    db_list = test_catalog.list_namespaces()
    for database_name in database_list:
        assert (database_name,) in db_list
    assert len(test_catalog.list_namespaces(list(database_list)[0])) == 0


@pytest.mark.integration
def test_drop_namespace(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str) -> None:
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()
    test_catalog.create_table((database_name, table_name), table_schema_nested)
    with pytest.raises(NamespaceNotEmptyError):
        test_catalog.drop_namespace(database_name)
    test_catalog.drop_table((database_name, table_name))
    test_catalog.drop_namespace(database_name)
    assert (database_name,) not in test_catalog.list_namespaces()


@pytest.mark.integration
def test_load_namespace_properties(test_catalog: Catalog, database_name: str) -> None:
    # TODO: Change warehouse_location to be arbitrary string, this test should not have a dependency or anything to do with S3
    # warehouse_location = get_s3_path(get_bucket_name())
    warehouse_location = "/a/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }

    test_catalog.create_namespace(database_name, test_properties)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


@pytest.mark.integration
def test_load_empty_namespace_properties(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    assert listed_properties == {"exists": "true"}


@pytest.mark.integration
def test_update_namespace_properties(test_catalog: Catalog, database_name: str) -> None:
    # TODO: Change warehouse_location to be arbitrary string, this test should not have a dependency or anything to do with S3
    # warehouse_location = get_s3_path(get_bucket_name())
    warehouse_location = "/a/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog.create_namespace(database_name, test_properties)
    update_report = test_catalog.update_namespace_properties(database_name, removals, updates)
    print(update_report)
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert "updated test description" == test_catalog.load_namespace_properties(database_name)["comment"]
