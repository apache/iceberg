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
import getpass as gt
import random
import string

import pytest

from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.schema import Schema

# early develop stage only, change this to a user with aws cli configured locally
MY_USERNAME = "jonasjiang"
RANDOM_LENGTH = 20


def get_random_table_name():
    prefix = "my_iceberg_table-"
    random_tag = "".join(random.choice(string.ascii_letters) for _ in range(RANDOM_LENGTH))
    return (prefix + random_tag).lower()


def get_random_tables(n):
    result = set()
    for _ in range(n):
        result.add(get_random_table_name())
    return result


def get_random_database_name():
    prefix = "my_iceberg_database-"
    random_tag = "".join(random.choice(string.ascii_letters) for _ in range(RANDOM_LENGTH))
    return (prefix + random_tag).lower()


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_create_table(table_schema_nested: Schema):
    table_name = get_random_table_name()
    identifier = ("myicebergtest", table_name)
    table = GlueCatalog("glue").create_table(
        identifier, table_schema_nested, f"s3://pythongluetest/myicebergtest.db/{table_name}"
    )
    assert table.identifier == identifier
    GlueCatalog("glue").drop_table(identifier)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_create_table_with_default_location(table_schema_nested: Schema):
    table_name = get_random_table_name()
    identifier = ("myicebergtest", table_name)
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == identifier
    GlueCatalog("glue").drop_table(identifier)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_create_table_with_invalid_database(table_schema_nested: Schema):
    table_name = get_random_table_name()
    identifier = ("invalid", table_name)
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier, table_schema_nested)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_create_table_with_invalid_location(table_schema_nested: Schema):
    table_name = get_random_table_name()
    identifier = ("myicebergtest", table_name)
    test_catalog = GlueCatalog("glue")
    with pytest.raises(ValueError):
        test_catalog.create_table(identifier, table_schema_nested)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_load_table():
    table = GlueCatalog("glue").load_table(("myicebergtest", "loadtest"))
    assert table.identifier == ("myicebergtest", "loadtest")


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_list_namespaces():
    db_list = GlueCatalog("glue").list_namespaces()
    assert ("listdatabasetest",) in db_list
    assert ("myicebergtest",) in db_list


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_drop_table(table_schema_nested):
    table_name = get_random_table_name()
    identifier = ("myicebergtest", table_name)
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == identifier
    test_catalog.drop_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_purge_table(table_schema_nested):
    test_drop_table(table_schema_nested)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_rename_table(table_schema_nested):
    table_name = get_random_table_name()
    new_table_name = f"rename-{table_name}"
    identifier = ("myicebergtest", table_name)
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == identifier
    new_identifier = ("myicebergtest", new_table_name)
    test_catalog.rename_table(identifier, new_identifier)
    new_table = test_catalog.load_table(new_identifier)
    assert new_table.identifier == new_identifier
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_create_namespace():
    database_name = get_random_database_name()
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()
    test_catalog.drop_namespace(database_name)
    assert (database_name,) not in test_catalog.list_namespaces()


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_drop_namespace():
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
    for database_name in test_catalog.list_namespaces():
        database_name = database_name[0]
        if "my_iceberg_database-" in database_name:
            for identifier in test_catalog.list_tables(database_name):
                test_catalog.drop_table(identifier)
            test_catalog.drop_namespace(database_name)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_load_namespaces_properties():
    warehouse_location = "s3://pythongluetest"
    database_name = get_random_database_name()
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
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
    test_catalog.drop_namespace(database_name)


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_list_tables(table_schema_nested):
    test_n = 2
    test_tables = get_random_tables(test_n)
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
    database_name = get_random_database_name()
    test_catalog.create_namespace(database_name)
    for table_name in test_tables:
        test_catalog.create_table((database_name, table_name), table_schema_nested)
    identifier_list = test_catalog.list_tables(database_name)
    assert len(identifier_list) == test_n
    for identifier in identifier_list:
        test_catalog.drop_table(identifier)
    test_catalog.drop_namespace(database_name)
    assert (database_name,) not in test_catalog.list_namespaces()


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_update_namespace_properties():
    warehouse_location = "s3://pythongluetest"
    database_name = get_random_database_name()
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
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
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert "updated test description" == test_catalog.load_namespace_properties(database_name)["comment"]
    test_catalog.drop_namespace(database_name)
