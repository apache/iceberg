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
from unittest.mock import patch, MagicMock

import botocore.exceptions

from pyiceberg.catalog import PropertiesUpdateSummary
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)

import random
import string


def get_randam_table_name():
    prefix = "my_iceberg_table-"
    random_tag = "".join(random.choice(string.ascii_letters) for _ in range(20))
    return (prefix + random_tag).lower()


def test_create_table(table_schema_nested: Schema):
    table_name = get_randam_table_name()
    identifier = ('reviewsjonas', table_name)
    table = GlueCatalog('glue').create_table(
        identifier,
        table_schema_nested,
        f"s3://myicebergtest/glueiceberg2/reviewsjonas.db/{table_name}"
    )
    assert table.identifier == identifier


def test_create_table_with_default_location(table_schema_nested: Schema):
    table_name = get_randam_table_name()
    identifier = ('reviewsjonas', table_name)
    test_catalog = GlueCatalog('glue', warehouse='s3://myicebergtest/glueiceberg2')
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == identifier


def test_create_table_with_invalid_database(table_schema_nested: Schema):
    try:
        table_name = get_randam_table_name()
        identifier = ('invalid', table_name)
        test_catalog = GlueCatalog('glue', warehouse='s3://myicebergtest/glueiceberg2')
        table = test_catalog.create_table(identifier, table_schema_nested)
    except NoSuchNamespaceError:
        return
    assert False


def test_create_table_with_invalid_location(table_schema_nested: Schema):
    try:
        table_name = get_randam_table_name()
        identifier = ('reviewsjonas', table_name)
        test_catalog = GlueCatalog('glue')
        table = test_catalog.create_table(identifier, table_schema_nested)
    except ValueError:
        return
    assert False


def test_load_table():
    table = GlueCatalog('glue').load_table(('reviews', 'book_reviews2'))
    assert table.identifier == ('reviews', 'book_reviews2')
