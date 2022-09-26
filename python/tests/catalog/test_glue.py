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


def test_get_namespaces():
    namespaces = GlueCatalog('glue', {}).list_namespaces()
    assert namespaces == []


def test_get_tables():
    tables = GlueCatalog('glue', {}).list_tables('nyc')
    assert tables == []


def test_create_namespace():
    # AccessDeniedException
    # EntityNotFoundException
    GlueCatalog('glue', {}).create_namespace('fokko', {'foo': 'bar'})


def test_drop_namespace():
    GlueCatalog('glue', {}).drop_namespace('fokko')


def test_load_namespace_properties():
    tables = GlueCatalog('glue', {}).load_namespace_properties('nyc')
    assert tables == []


def test_update_namespace_properties():
    tables = GlueCatalog('glue', {}).update_namespace_properties('nyc', updates={'foo': 'bar'})
    assert tables == PropertiesUpdateSummary(removed=[], updated=['foo'], missing=[])


@patch("time.time", MagicMock(return_value=12345))
@patch("uuid.uuid4", MagicMock(return_value="01234567-0123-0123-0123-0123456789ab"))
def test_create_table(table_schema_nested: Schema):
    table = GlueCatalog('glue', {}).create_table(('reviews', 'createTestDb'), table_schema_nested, "s3://myicebergtest/glueiceberg2/test.db/")
    assert table is None


def test_load_table_not_found(table_schema_nested: Schema):
    testGlueCatalog = GlueCatalog('glue', {})
    table = testGlueCatalog.load_table(('fokko', 'test'))
    assert table is None


def test_load_table():
    table = GlueCatalog('glue', {}).load_table(('reviews', 'book_reviews2'))
    assert not (table is None)


def test_rename_table():
    table = GlueCatalog('glue', {}).rename_table(
        ('fokko', 'test'),
        ('fokko', 'test2')
    )
    assert table is None

def test_drop_table():
    GlueCatalog('glue', {}).drop_table(('reviews', 'create_test'))
