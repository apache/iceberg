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
# pylint: disable=redefined-outer-name,unused-argument
from uuid import UUID

import pytest
from requests_mock import Mocker

import pyiceberg
from pyiceberg.catalog import PropertiesUpdateSummary, Table
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    OAuthError,
    TableAlreadyExistsError,
)
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV1
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform, TruncateTransform
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    NestedField,
    StringType,
)

TEST_URI = "https://iceberg-test-catalog/"
TEST_CREDENTIALS = "client:secret"
TEST_TOKEN = "some_jwt_token"
TEST_HEADERS = {
    "Content-type": "application/json",
    "X-Client-Version": "0.14.1",
    "User-Agent": f"PyIceberg/{pyiceberg.__version__}",
    "Authorization": f"Bearer {TEST_TOKEN}",
}
OAUTH_TEST_HEADERS = {
    "Content-type": "application/x-www-form-urlencoded",
}


@pytest.fixture
def rest_mock(requests_mock: Mocker) -> Mocker:
    """Takes the default requests_mock and adds the config endpoint to it

    This endpoint is called when initializing the rest catalog
    """
    requests_mock.get(
        f"{TEST_URI}v1/config",
        json={"defaults": {}, "overrides": {}},
        status_code=200,
    )
    return requests_mock


def test_no_uri_supplied() -> None:
    with pytest.raises(KeyError):
        RestCatalog("production")


def test_token_200(rest_mock: Mocker) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/oauth/tokens",
        json={
            "access_token": TEST_TOKEN,
            "token_type": "Bearer",
            "expires_in": 86400,
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
        },
        status_code=200,
        request_headers=OAUTH_TEST_HEADERS,
    )
    assert (
        RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS).session.headers["Authorization"] == f"Bearer {TEST_TOKEN}"
    )


def test_config_200(requests_mock: Mocker) -> None:
    requests_mock.get(
        f"{TEST_URI}v1/config",
        json={"defaults": {}, "overrides": {}},
        status_code=200,
    )
    requests_mock.post(
        f"{TEST_URI}v1/oauth/tokens",
        json={
            "access_token": TEST_TOKEN,
            "token_type": "Bearer",
            "expires_in": 86400,
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
        },
        status_code=200,
        request_headers=OAUTH_TEST_HEADERS,
    )
    RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS, warehouse="s3://some-bucket")

    assert requests_mock.called
    assert requests_mock.call_count == 2

    history = requests_mock.request_history
    assert history[1].method == "GET"
    assert history[1].url == "https://iceberg-test-catalog/v1/config?warehouse=s3%3A%2F%2Fsome-bucket"


def test_token_400(rest_mock: Mocker) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/oauth/tokens",
        json={"error": "invalid_client", "error_description": "Credentials for key invalid_key do not match"},
        status_code=400,
        request_headers=OAUTH_TEST_HEADERS,
    )

    with pytest.raises(OAuthError) as e:
        RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS)
    assert str(e.value) == "invalid_client: Credentials for key invalid_key do not match"


def test_token_401(rest_mock: Mocker) -> None:
    message = "invalid_client"
    rest_mock.post(
        f"{TEST_URI}v1/oauth/tokens",
        json={"error": "invalid_client", "error_description": "Unknown or invalid client"},
        status_code=401,
        request_headers=OAUTH_TEST_HEADERS,
    )

    with pytest.raises(OAuthError) as e:
        RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS)
    assert message in str(e.value)


def test_list_tables_200(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}/tables",
        json={"identifiers": [{"namespace": ["examples"], "name": "fooshare"}]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )

    assert RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).list_tables(namespace) == [("examples", "fooshare")]


def test_list_tables_404(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}/tables",
        json={
            "error": {
                "message": "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchNamespaceError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).list_tables(namespace)
    assert "Namespace does not exist" in str(e.value)


def test_list_namespaces_200(rest_mock: Mocker) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces",
        json={"namespaces": [["default"], ["examples"], ["fokko"], ["system"]]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    assert RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).list_namespaces() == [
        ("default",),
        ("examples",),
        ("fokko",),
        ("system",),
    ]


def test_list_namespace_with_parent_200(rest_mock: Mocker) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces?parent=accounting",
        json={"namespaces": [["tax"]]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    assert RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).list_namespaces(("accounting",)) == [
        ("accounting", "tax"),
    ]


def test_create_namespace_200(rest_mock: Mocker) -> None:
    namespace = "leden"
    rest_mock.post(
        f"{TEST_URI}v1/namespaces",
        json={"namespace": [namespace], "properties": {}},
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_namespace(namespace)


def test_create_namespace_409(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.post(
        f"{TEST_URI}v1/namespaces",
        json={
            "error": {
                "message": "Namespace already exists: fokko in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "AlreadyExistsException",
                "code": 409,
            }
        },
        status_code=409,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NamespaceAlreadyExistsError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_namespace(namespace)
    assert "Namespace already exists" in str(e.value)


def test_drop_namespace_404(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/{namespace}",
        json={
            "error": {
                "message": "Namespace does not exist: leden in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchNamespaceError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_namespace(namespace)
    assert "Namespace does not exist" in str(e.value)


def test_load_namespace_properties_200(rest_mock: Mocker) -> None:
    namespace = "leden"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}",
        json={"namespace": ["fokko"], "properties": {"prop": "yes"}},
        status_code=204,
        request_headers=TEST_HEADERS,
    )
    assert RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_namespace_properties(namespace) == {"prop": "yes"}


def test_load_namespace_properties_404(rest_mock: Mocker) -> None:
    namespace = "leden"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}",
        json={
            "error": {
                "message": "Namespace does not exist: fokko22 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchNamespaceError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_namespace_properties(namespace)
    assert "Namespace does not exist" in str(e.value)


def test_update_namespace_properties_200(rest_mock: Mocker) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/fokko/properties",
        json={"removed": [], "updated": ["prop"], "missing": ["abc"]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    response = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).update_namespace_properties(
        ("fokko",), {"abc"}, {"prop": "yes"}
    )

    assert response == PropertiesUpdateSummary(removed=[], updated=["prop"], missing=["abc"])


def test_update_namespace_properties_404(rest_mock: Mocker) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/fokko/properties",
        json={
            "error": {
                "message": "Namespace does not exist: does_not_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchNamespaceError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).update_namespace_properties(("fokko",), {"abc"}, {"prop": "yes"})
    assert "Namespace does not exist" in str(e.value)


def test_load_table_200(rest_mock: Mocker) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/fokko/tables/table",
        json={
            "metadata-location": "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
            "metadata": {
                "format-version": 1,
                "table-uuid": "b55d9dda-6561-423a-8bfc-787980ce421f",
                "location": "s3://warehouse/database/table",
                "last-updated-ms": 1646787054459,
                "last-column-id": 2,
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": False, "type": "int"},
                        {"id": 2, "name": "data", "required": False, "type": "string"},
                    ],
                },
                "current-schema-id": 0,
                "schemas": [
                    {
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [
                            {"id": 1, "name": "id", "required": False, "type": "int"},
                            {"id": 2, "name": "data", "required": False, "type": "string"},
                        ],
                    }
                ],
                "partition-spec": [],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "last-partition-id": 999,
                "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "properties": {"owner": "bryan", "write.metadata.compression-codec": "gzip"},
                "current-snapshot-id": 3497810964824022504,
                "refs": {"main": {"snapshot-id": 3497810964824022504, "type": "branch"}},
                "snapshots": [
                    {
                        "snapshot-id": 3497810964824022504,
                        "timestamp-ms": 1646787054459,
                        "summary": {
                            "operation": "append",
                            "spark.app.id": "local-1646787004168",
                            "added-data-files": "1",
                            "added-records": "1",
                            "added-files-size": "697",
                            "changed-partition-count": "1",
                            "total-records": "1",
                            "total-files-size": "697",
                            "total-data-files": "1",
                            "total-delete-files": "0",
                            "total-position-deletes": "0",
                            "total-equality-deletes": "0",
                        },
                        "manifest-list": "s3://warehouse/database/table/metadata/snap-3497810964824022504-1-c4f68204-666b-4e50-a9df-b10c34bf6b82.avro",
                        "schema-id": 0,
                    }
                ],
                "snapshot-log": [{"timestamp-ms": 1646787054459, "snapshot-id": 3497810964824022504}],
                "metadata-log": [
                    {
                        "timestamp-ms": 1646787031514,
                        "metadata-file": "s3://warehouse/database/table/metadata/00000-88484a1c-00e5-4a07-a787-c0e7aeffa805.gz.metadata.json",
                    }
                ],
            },
            "config": {"client.factory": "io.tabular.iceberg.catalog.TabularAwsClientFactory", "region": "us-west-2"},
        },
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    actual = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_table(("fokko", "table"))
    expected = Table(
        identifier=("rest", "fokko", "table"),
        metadata_location="s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
        metadata=TableMetadataV1(
            location="s3://warehouse/database/table",
            table_uuid=UUID("b55d9dda-6561-423a-8bfc-787980ce421f"),
            last_updated_ms=1646787054459,
            last_column_id=2,
            schemas=[
                Schema(
                    NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
                    NestedField(field_id=2, name="data", field_type=StringType(), required=False),
                    schema_id=0,
                    identifier_field_ids=[],
                )
            ],
            current_schema_id=0,
            default_spec_id=0,
            last_partition_id=999,
            properties={"owner": "bryan", "write.metadata.compression-codec": "gzip"},
            current_snapshot_id=3497810964824022504,
            snapshots=[
                Snapshot(
                    snapshot_id=3497810964824022504,
                    parent_snapshot_id=None,
                    sequence_number=None,
                    timestamp_ms=1646787054459,
                    manifest_list="s3://warehouse/database/table/metadata/snap-3497810964824022504-1-c4f68204-666b-4e50-a9df-b10c34bf6b82.avro",
                    summary=Summary(
                        operation=Operation.APPEND,
                        **{  # type: ignore
                            "spark.app.id": "local-1646787004168",
                            "added-data-files": "1",
                            "added-records": "1",
                            "added-files-size": "697",
                            "changed-partition-count": "1",
                            "total-records": "1",
                            "total-files-size": "697",
                            "total-data-files": "1",
                            "total-delete-files": "0",
                            "total-position-deletes": "0",
                            "total-equality-deletes": "0",
                        },
                    ),
                    schema_id=0,
                )
            ],
            snapshot_log=[{"timestamp-ms": 1646787054459, "snapshot-id": 3497810964824022504}],
            metadata_log=[
                {
                    "timestamp-ms": 1646787031514,
                    "metadata-file": "s3://warehouse/database/table/metadata/00000-88484a1c-00e5-4a07-a787-c0e7aeffa805.gz.metadata.json",
                }
            ],
            sort_orders=[SortOrder(order_id=0)],
            default_sort_order_id=0,
            refs={
                "main": SnapshotRef(
                    snapshot_id=3497810964824022504,
                    snapshot_ref_type=SnapshotRefType.BRANCH,
                    min_snapshots_to_keep=None,
                    max_snapshot_age_ms=None,
                    max_ref_age_ms=None,
                )
            },
            format_version=1,
            schema_=Schema(
                NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
                NestedField(field_id=2, name="data", field_type=StringType(), required=False),
                schema_id=0,
                identifier_field_ids=[],
            ),
            partition_spec=[],
        ),
        io=load_file_io(),
    )
    assert actual == expected


def test_load_table_404(rest_mock: Mocker) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/fokko/tables/does_not_exists",
        json={
            "error": {
                "message": "Table does not exist: examples.does_not_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceErrorException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )

    with pytest.raises(NoSuchTableError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_table(("fokko", "does_not_exists"))
    assert "Table does not exist" in str(e.value)


def test_drop_table_404(rest_mock: Mocker) -> None:
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/fokko/tables/does_not_exists",
        json={
            "error": {
                "message": "Table does not exist: fokko.does_not_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceErrorException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )

    with pytest.raises(NoSuchTableError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_table(("fokko", "does_not_exists"))
    assert "Table does not exist" in str(e.value)


def test_create_table_200(rest_mock: Mocker, table_schema_simple: Schema) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/fokko/tables",
        json={
            "metadata-location": "s3://warehouse/database/table/metadata.json",
            "metadata": {
                "format-version": 1,
                "table-uuid": "bf289591-dcc0-4234-ad4f-5c3eed811a29",
                "location": "s3://warehouse/database/table",
                "last-updated-ms": 1657810967051,
                "last-column-id": 3,
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "identifier-field-ids": [2],
                    "fields": [
                        {"id": 1, "name": "foo", "required": False, "type": "string"},
                        {"id": 2, "name": "bar", "required": True, "type": "int"},
                        {"id": 3, "name": "baz", "required": False, "type": "boolean"},
                    ],
                },
                "current-schema-id": 0,
                "schemas": [
                    {
                        "type": "struct",
                        "schema-id": 0,
                        "identifier-field-ids": [2],
                        "fields": [
                            {"id": 1, "name": "foo", "required": False, "type": "string"},
                            {"id": 2, "name": "bar", "required": True, "type": "int"},
                            {"id": 3, "name": "baz", "required": False, "type": "boolean"},
                        ],
                    }
                ],
                "partition-spec": [],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "properties": {
                    "write.delete.parquet.compression-codec": "zstd",
                    "write.metadata.compression-codec": "gzip",
                    "write.summary.partition-limit": "100",
                    "write.parquet.compression-codec": "zstd",
                },
                "current-snapshot-id": -1,
                "refs": {},
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
            },
            "config": {
                "client.factory": "io.tabular.iceberg.catalog.TabularAwsClientFactory",
                "region": "us-west-2",
            },
        },
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    table = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_table(
        identifier=("fokko", "fokko2"),
        schema=table_schema_simple,
        location=None,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
        ),
        sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
        properties={"owner": "fokko"},
    )
    assert table == Table(
        identifier=("rest", "fokko", "fokko2"),
        metadata_location="s3://warehouse/database/table/metadata.json",
        metadata=TableMetadataV1(
            location="s3://warehouse/database/table",
            table_uuid=UUID("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            last_updated_ms=1657810967051,
            last_column_id=3,
            schemas=[
                Schema(
                    NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                    NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
                    schema_id=0,
                    identifier_field_ids=[2],
                )
            ],
            current_schema_id=0,
            default_spec_id=0,
            last_partition_id=999,
            properties={
                "write.delete.parquet.compression-codec": "zstd",
                "write.metadata.compression-codec": "gzip",
                "write.summary.partition-limit": "100",
                "write.parquet.compression-codec": "zstd",
            },
            current_snapshot_id=None,
            snapshots=[],
            snapshot_log=[],
            metadata_log=[],
            sort_orders=[SortOrder(order_id=0)],
            default_sort_order_id=0,
            refs={},
            format_version=1,
            schema_=Schema(
                NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
                schema_id=0,
                identifier_field_ids=[2],
            ),
            partition_spec=[],
        ),
        io=load_file_io(),
    )


def test_create_table_409(rest_mock: Mocker, table_schema_simple: Schema) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/fokko/tables",
        json={
            "error": {
                "message": "Table already exists: fokko.already_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "AlreadyExistsException",
                "code": 409,
            }
        },
        status_code=409,
        request_headers=TEST_HEADERS,
    )

    with pytest.raises(TableAlreadyExistsError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_table(
            identifier=("fokko", "fokko2"),
            schema=table_schema_simple,
            location=None,
            partition_spec=PartitionSpec(
                PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id")
            ),
            sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
            properties={"owner": "fokko"},
        )
    assert "Table already exists" in str(e.value)


def test_delete_namespace_204(rest_mock: Mocker) -> None:
    namespace = "example"
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/{namespace}",
        json={},
        status_code=204,
        request_headers=TEST_HEADERS,
    )
    RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_namespace(namespace)


def test_delete_table_204(rest_mock: Mocker) -> None:
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/example/tables/fokko",
        json={},
        status_code=204,
        request_headers=TEST_HEADERS,
    )
    RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_table(("example", "fokko"))


def test_delete_table_404(rest_mock: Mocker) -> None:
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/example/tables/fokko",
        json={
            "error": {
                "message": "Table does not exist: fokko.fokko2 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchTableException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchTableError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_table(("example", "fokko"))
    assert "Table does not exist" in str(e.value)


def test_create_table_missing_namespace(rest_mock: Mocker, table_schema_simple: Schema) -> None:
    table = "table"
    with pytest.raises(NoSuchTableError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_table(table, table_schema_simple)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


def test_load_table_invalid_namespace(rest_mock: Mocker) -> None:
    table = "table"
    with pytest.raises(NoSuchTableError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


def test_drop_table_invalid_namespace(rest_mock: Mocker) -> None:
    table = "table"
    with pytest.raises(NoSuchTableError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


def test_purge_table_invalid_namespace(rest_mock: Mocker) -> None:
    table = "table"
    with pytest.raises(NoSuchTableError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).purge_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


def test_create_namespace_invalid_namespace(rest_mock: Mocker) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_namespace(())
    assert "Empty namespace identifier" in str(e.value)


def test_drop_namespace_invalid_namespace(rest_mock: Mocker) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_namespace(())
    assert "Empty namespace identifier" in str(e.value)


def test_load_namespace_properties_invalid_namespace(rest_mock: Mocker) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_namespace_properties(())
    assert "Empty namespace identifier" in str(e.value)


def test_update_namespace_properties_invalid_namespace(rest_mock: Mocker) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).update_namespace_properties(())
    assert "Empty namespace identifier" in str(e.value)


def test_request_session_with_ssl_ca_bundle() -> None:
    # Given
    catalog_properties = {
        "uri": TEST_URI,
        "token": TEST_TOKEN,
        "ssl": {
            "cabundle": "path_to_ca_bundle",
        },
    }
    with pytest.raises(OSError) as e:
        # Missing namespace
        RestCatalog("rest", **catalog_properties)  # type: ignore
    assert "Could not find a suitable TLS CA certificate bundle, invalid path: path_to_ca_bundle" in str(e.value)


def test_request_session_with_ssl_client_cert() -> None:
    # Given
    catalog_properties = {
        "uri": TEST_URI,
        "token": TEST_TOKEN,
        "ssl": {
            "client": {
                "cert": "path_to_client_cert",
                "key": "path_to_client_key",
            }
        },
    }
    with pytest.raises(OSError) as e:
        # Missing namespace
        RestCatalog("rest", **catalog_properties)  # type: ignore
    assert "Could not find the TLS certificate file, invalid path: path_to_client_cert" in str(e.value)
