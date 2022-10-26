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
import random
import re
import string

import pytest
from moto import mock_glue

from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, TableAlreadyExistsError

BUCKET_NAME = "test_bucket"
RANDOM_LENGTH = 20
LIST_TEST_NUMBER = 100
table_metadata_location_regex = re.compile(
    r"s3://test_bucket/my_iceberg_database-[a-z]{20}.db/my_iceberg_table-[a-z]{20}/metadata/00000-[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}.metadata.json"
)


def get_random_table_name():
    prefix = "my_iceberg_table-"
    random_tag = "".join(random.choice(string.ascii_letters) for _ in range(RANDOM_LENGTH))
    return (prefix + random_tag).lower()


def get_random_tables(n):
    result = []
    for _ in range(n):
        result.append(get_random_table_name())
    return result


def get_random_database_name():
    prefix = "my_iceberg_database-"
    random_tag = "".join(random.choice(string.ascii_letters) for _ in range(RANDOM_LENGTH))
    return (prefix + random_tag).lower()


@pytest.fixture(name="_bucket_initialize")
def fixture_s3_bucket(_s3):
    _s3.create_bucket(Bucket=BUCKET_NAME)


@mock_glue
def test_create_table_with_database_location(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue")
    test_catalog.create_namespace(namespace=database_name, properties={"location": f"s3://{BUCKET_NAME}/{database_name}.db"})
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == identifier
    assert table_metadata_location_regex.match(table.metadata_location)


@mock_glue
def test_create_table_with_default_warehouse(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == identifier
    assert table_metadata_location_regex.match(table.metadata_location)


@mock_glue
def test_create_table_with_given_location(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue")
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(
        identifier=identifier, schema=table_schema_nested, location=f"s3://{BUCKET_NAME}/{database_name}.db/{table_name}"
    )
    assert table.identifier == identifier
    assert table_metadata_location_regex.match(table.metadata_location)


@mock_glue
def test_create_table_with_no_location(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue")
    test_catalog.create_namespace(namespace=database_name)
    with pytest.raises(ValueError):
        test_catalog.create_table(identifier=identifier, schema=table_schema_nested)


@mock_glue
def test_create_table_with_no_database(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue")
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier=identifier, schema=table_schema_nested)


@mock_glue
def test_create_duplicated_table(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_table(identifier, table_schema_nested)


@mock_glue
def test_load_table(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == identifier
    assert table_metadata_location_regex.match(table.metadata_location)


@mock_glue
def test_load_non_exist_table(_bucket_initialize, _patch_aiobotocore):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_glue
def test_drop_table(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == identifier
    assert table_metadata_location_regex.match(table.metadata_location)
    test_catalog.drop_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_glue
def test_drop_non_exist_table(_bucket_initialize, _patch_aiobotocore):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", warehouse=f"s3://{BUCKET_NAME}")
    with pytest.raises(NoSuchTableError):
        test_catalog.drop_table(identifier)


@mock_glue
def test_purge_table(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    test_drop_table(_bucket_initialize, _patch_aiobotocore, table_schema_nested)


@mock_glue
def test_rename_table(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_name = get_random_table_name()
    new_table_name = get_random_table_name()
    identifier = (database_name, table_name)
    new_identifier = (database_name, new_table_name)
    test_catalog = GlueCatalog("glue", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == identifier
    assert table_metadata_location_regex.match(table.metadata_location)
    test_catalog.rename_table(identifier, new_identifier)
    new_table = test_catalog.load_table(new_identifier)
    assert new_table.identifier == new_identifier
    # the metadata_location should not change
    assert new_table.metadata_location == table.metadata_location
    # old table should be dropped
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_glue
def test_list_tables(_bucket_initialize, _patch_aiobotocore, table_schema_nested):
    database_name = get_random_database_name()
    table_list = get_random_tables(LIST_TEST_NUMBER)
    test_catalog = GlueCatalog("glue", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    for i in range(len(table_list)):
        test_catalog.create_table((database_name, table_list[i]), table_schema_nested)
    loaded_table_list = test_catalog.list_tables(database_name)
    for i in range(len(table_list)):
        assert (database_name, table_list[i]) in loaded_table_list


@mock_glue
def test_unit_list_namespaces(_bucket_initialize, _patch_aiobotocore):
    database_name = "testDatabase"
    test_catalog = GlueCatalog("glue")
    test_catalog.create_namespace(namespace=database_name)
    identifiers = test_catalog.list_namespaces()
    assert len(identifiers) == 1
    assert identifiers[0] == (database_name,)
