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
from pyiceberg.catalog.rest import RestCatalog


def test_get_token():
    RestCatalog('rest', {})._fetch_access_token()


def test_get_config():
    RestCatalog('rest', {})._fetch_config()


def test_list_tables():
    RestCatalog('rest', {}).list_tables('personal')


def test_list_namespaces():
    RestCatalog('rest', {}).list_namespaces()


def test_create_namespaces():
    RestCatalog('rest', {}).create_namespace(('fokko',))


def test_drop_namespaces():
    RestCatalog('rest', {}).drop_namespace(('fokko',))


def test_load_namespace_properties():
    RestCatalog('rest', {}).load_namespace_properties(('fokko',))


def test_update_namespace_properties():
    RestCatalog('rest', {}).update_namespace_properties(('fokko',), {'abc'}, {'prop': 'yes'})


def test_create_table():


