import os
from typing import Generator

import pytest
from sqlalchemy.exc import ArgumentError

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchPropertyException
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import IdentityTransform
from tests.conftest import get_bucket_name, get_s3_path

# @pytest.fixture(scope="session")
# def test_db() -> str:
#     db_file = tempfile.mktemp(suffix=".db")
#     conn = sqlite3.connect(db_file)
#     cursor = conn.cursor()

#     # yield conn
#     cursor.close()
#     conn.close()
#     yield db_file
#     os.remove(db_file)


@pytest.fixture(name="test_catalog", scope="module")
def fixture_test_catalog() -> Generator[SqlCatalog, None, None]:
    os.environ["AWS_TEST_BUCKET"] = "warehouse"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
    props = {
        "uri": "sqlite+pysqlite:///:memory:",
        # "warehouse": "file:///tmp",
        "warehouse": get_s3_path(get_bucket_name()),
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
    test_catalog = SqlCatalog("test_sql_catalog", **props)
    test_catalog.initialize_tables()
    yield test_catalog


# def test_create_namespace(test_db: str):
#     props = {
#         "uri": f"file:{test_db}"
#     }
#     test_catalog = JDBCCatalog("test_ddb_catalog", **props)
#     test_catalog.initialize_catalog_tables()
#     test_catalog.create_namespace(namespace="database_name")
#     test_catalog.list_namespaces()


def test_creation_with_no_uri() -> None:
    with pytest.raises(NoSuchPropertyException):
        SqlCatalog("test_ddb_catalog", not_uri="unused")


def test_creation_with_unsupported_uri() -> None:
    with pytest.raises(ArgumentError):
        SqlCatalog("test_ddb_catalog", uri="unsupported:xxx")


def test_initialize(test_catalog: SqlCatalog) -> None:
    # Second initialization should not fail even if tables are already created
    test_catalog.initialize_tables()
    test_catalog.initialize_tables()


def test_create_table_default_sort_order(
    test_catalog: SqlCatalog, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.sort_order().order_id == 0, "Order ID must match"
    assert table.sort_order().is_unsorted is True, "Order must be unsorted"


def test_create_table_custom_sort_order(
    test_catalog: SqlCatalog, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    order = SortOrder(SortField(source_id=2, transform=IdentityTransform(), null_order=NullOrder.NULLS_FIRST))
    table = test_catalog.create_table(identifier, table_schema_nested, sort_order=order)
    given_sort_order = table.sort_order()
    assert given_sort_order.order_id == 1, "Order ID must match"
    assert len(given_sort_order.fields) == 1, "Order must have 1 field"
    assert given_sort_order.fields[0].direction == SortDirection.ASC, "Direction must match"
    assert given_sort_order.fields[0].null_order == NullOrder.NULLS_FIRST, "Null order must match"
    assert isinstance(given_sort_order.fields[0].transform, IdentityTransform), "Transform must match"
