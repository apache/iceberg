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
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.schema import Schema

# early develop stage only, change this to a user with aws cli configured locally
MY_USERNAME = "jonasjiang"


def get_random_table_name():
    prefix = "my_iceberg_table-"
    random_tag = "".join(random.choice(string.ascii_letters) for _ in range(20))
    return (prefix + random_tag).lower()


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_create_table(table_schema_nested: Schema):
    table_name = get_random_table_name()
    identifier = ("myicebergtest", table_name)
    table = GlueCatalog("glue").create_table(
        identifier, table_schema_nested, f"s3://pythongluetest/myicebergtest.db/{table_name}"
    )
    assert table.identifier == identifier


@pytest.mark.skipif(gt.getuser() != MY_USERNAME, reason="currently need aws account, will be unit test later")
def test_create_table_with_default_location(table_schema_nested: Schema):
    table_name = get_random_table_name()
    identifier = ("myicebergtest", table_name)
    test_catalog = GlueCatalog("glue", warehouse="s3://pythongluetest")
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == identifier


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
    assert db_list == [("listdatabasetest",), ("myicebergtest",)]
