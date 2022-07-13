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
import pytest
import requests_mock

from pyiceberg.catalog.rest import RestCatalog, UpdateNamespacePropertiesResponse
from pyiceberg.exceptions import (
    AlreadyExistsError,
    BadCredentialsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)

TEST_HOST = "https://iceberg-test-catalog/"


def test_token_200():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )

        assert RestCatalog("rest", {}, TEST_HOST).token == token


def test_token_401():
    with requests_mock.Mocker() as m:
        message = "Invalid client ID: abc"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "error": {
                    "message": message,
                    "type": "BadCredentialsException",
                    "code": 401,
                }
            },
            status_code=401,
        )

        with pytest.raises(BadCredentialsError) as e:
            RestCatalog("rest", {}, TEST_HOST)
        assert message in str(e.value)


def test_list_tables_200():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        namespace = "examples"
        m.get(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces/{namespace}/tables",
            json={"identifiers": [{"namespace": ["examples"], "name": "fooshare"}]},
            status_code=200,
        )

        assert RestCatalog("rest", {}, TEST_HOST).list_tables(namespace) == [("examples", "fooshare")]


def test_list_tables_404():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        namespace = "examples"

        m.get(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces/{namespace}/tables",
            json={
                "error": {
                    "message": "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                    "type": "NoSuchNamespaceException",
                    "code": 404,
                }
            },
            status_code=404,
        )
        with pytest.raises(NoSuchTableError) as e:
            RestCatalog("rest", {}, TEST_HOST).list_tables(namespace)
        assert "Namespace does not exist" in str(e.value)


def test_list_namespaces_200():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces",
            json={"namespaces": [["default"], ["examples"], ["fokko"], ["system"]]},
            status_code=200,
        )
        assert RestCatalog("rest", {}, TEST_HOST).list_namespaces() == ["default", "examples", "fokko", "system"]


def test_create_namespace_200():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        namespace = "leden"
        m.post(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces",
            json={"namespace": [namespace], "properties": {}},
            status_code=200,
        )
        RestCatalog("rest", {}, TEST_HOST).create_namespace(namespace)


def test_create_namespace_409():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        namespace = "examples"
        m.post(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces",
            json={
                "error": {
                    "message": "Namespace already exists: fokko in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                    "type": "AlreadyExistsException",
                    "code": 409,
                }
            },
            status_code=409,
        )
        with pytest.raises(AlreadyExistsError) as e:
            RestCatalog("rest", {}, TEST_HOST).create_namespace(namespace)
        assert "Namespace already exists" in str(e.value)


def test_delete_namespace_204():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        namespace = "leden"
        m.delete(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces/{namespace}",
            json={},
            status_code=204,
        )
        RestCatalog("rest", {}, TEST_HOST).drop_namespace(namespace)


def test_drop_namespace_404():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        namespace = "examples"
        m.delete(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces/{namespace}",
            json={
                "error": {
                    "message": "Namespace does not exist: leden in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                    "type": "NoSuchNamespaceException",
                    "code": 404,
                }
            },
            status_code=404,
        )
        with pytest.raises(NoSuchNamespaceError) as e:
            RestCatalog("rest", {}, TEST_HOST).drop_namespace(namespace)
        assert "Namespace does not exist" in str(e.value)


def test_load_namespace_properties_200():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9b-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        namespace = "leden"
        m.get(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces/{namespace}",
            json={"namespace": ["fokko"], "properties": {"prop": "yes"}},
            status_code=204,
        )
        assert RestCatalog("rest", {}, TEST_HOST).load_namespace_properties(namespace) == {"prop": "yes"}


def test_load_namespace_properties_404():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9b-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        namespace = "leden"
        m.get(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces/{namespace}",
            json={
                "error": {
                    "message": "Namespace does not exist: fokko22 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                    "type": "NoSuchNamespaceException",
                    "code": 404,
                }
            },
            status_code=404,
        )
        with pytest.raises(NoSuchNamespaceError) as e:
            RestCatalog("rest", {}, TEST_HOST).load_namespace_properties(namespace)
        assert "Namespace does not exist" in str(e.value)


def test_update_namespace_properties_200():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9b-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        m.post(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces/fokko/properties",
            json={"removed": [], "updated": ["prop"], "missing": ["abc"]},
            status_code=200,
        )
        response = RestCatalog("rest", {}, TEST_HOST).update_namespace_properties(("fokko",), {"abc"}, {"prop": "yes"})

        assert response == UpdateNamespacePropertiesResponse(removed=[], updated=["prop"], missing=["abc"])


def test_update_namespace_properties_404():
    with requests_mock.Mocker() as m:
        token = "eyJ0eXAiOiJK"
        m.post(
            f"{TEST_HOST}oauth/tokens",
            json={
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "warehouse_id": "8bcb0838-50fc-472d-9b-8feb89ef5f1e",
                "region": "us-west-2",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            },
            status_code=200,
        )
        m.get(
            f"{TEST_HOST}config",
            json={"defaults": {}, "overrides": {"prefix": "oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e"}},
            status_code=200,
        )
        m.post(
            f"{TEST_HOST}oss/warehouses/8bcb0838-22vo-472d-1925-8feb89lidf1e/namespaces/fokko/properties",
            json={
                "error": {
                    "message": "Namespace does not exist: does_not_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                    "type": "NoSuchNamespaceException",
                    "code": 404,
                }
            },
            status_code=404,
        )
        with pytest.raises(NoSuchNamespaceError) as e:
            RestCatalog("rest", {}, TEST_HOST).update_namespace_properties(("fokko",), {"abc"}, {"prop": "yes"})
        assert "Namespace does not exist" in str(e.value)


def test_update_namespace_properties():
    RestCatalog("rest", {}, "https://api.dev.tabulardata.io/ws/v1/").update_namespace_properties(
        ("does_not_exists",), {"abc"}, {"prop": "yes"}
    )

def test_load_table():
    RestCatalog("rest", {}, "https://api.dev.tabulardata.io/ws/v1/").load_table(
        ("examples", "fooshare")
    )
