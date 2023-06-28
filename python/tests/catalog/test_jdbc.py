import pytest
import tempfile
import sqlite3
import os
from pyiceberg.catalog.jdbc import JDBCCatalog
from pyiceberg.exceptions import NoSuchPropertyException

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
        JDBCCatalog("test_ddb_catalog", uri=None)

    with pytest.raises(NoSuchPropertyException):
        JDBCCatalog("test_ddb_catalog", not_uri="unused")


def test_creation_with_unsupported_uri() -> None:
    with pytest.raises(ValueError):
        JDBCCatalog("test_ddb_catalog", uri="unsupported:xxx")
