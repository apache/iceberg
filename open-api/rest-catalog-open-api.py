# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Extra, Field


class ErrorModel(BaseModel):
    """
    JSON error payload returned in a response with further details on the error
    """

    message: str = Field(..., description='Human-readable error message')
    type: str = Field(
        ...,
        description='Internal type definition of the error',
        example='NoSuchNamespaceException',
    )
    code: int = Field(
        ..., description='HTTP response code', example=404, ge=400, le=600
    )
    stack: Optional[List[str]] = None


class CatalogConfig(BaseModel):
    """
    Server-provided configuration for the catalog.
    """

    overrides: Dict[str, str] = Field(
        ...,
        description='Properties that should be used to override client configuration; applied after defaults and client configuration.',
    )
    defaults: Dict[str, str] = Field(
        ...,
        description='Properties that should be used as default configuration; applied before client configuration.',
    )


class UpdateNamespacePropertiesRequest(BaseModel):
    removals: Optional[List[str]] = Field(
        None, example=['department', 'access_group'], unique_items=True
    )
    updates: Optional[Dict[str, str]] = Field(
        None, example={'owner': 'Hank Bendickson'}
    )


class Namespace(BaseModel):
    """
    Reference to one or more levels of a namespace
    """

    __root__: List[str] = Field(
        ...,
        description='Reference to one or more levels of a namespace',
        example=['accounting', 'tax'],
    )


class TableIdentifier(BaseModel):
    namespace: Namespace
    name: str


class PrimitiveType(BaseModel):
    __root__: str = Field(..., example=['long', 'string', 'fixed[16]', 'decimal(10,2)'])


class Type1(Enum):
    struct = 'struct'


class Type2(Enum):
    list = 'list'


class Type3(Enum):
    map = 'map'


class ExpressionType(BaseModel):
    __root__: str = Field(
        ...,
        example=[
            'eq',
            'and',
            'or',
            'not',
            'in',
            'not-in',
            'lt',
            'lt-eq',
            'gt',
            'gt-eq',
            'not-eq',
            'starts-with',
            'not-starts-with',
            'is-null',
            'not-null',
            'is-nan',
            'not-nan',
        ],
    )


class Reference(BaseModel):
    __root__: str = Field(..., example=['column-name'])


class Type4(Enum):
    transform = 'transform'


class Transform(BaseModel):
    __root__: str = Field(
        ...,
        example=[
            'identity',
            'year',
            'month',
            'day',
            'hour',
            'bucket[256]',
            'truncate[16]',
        ],
    )


class PartitionField(BaseModel):
    field_id: Optional[int] = Field(None, alias='field-id')
    source_id: int = Field(..., alias='source-id')
    name: str
    transform: Transform


class PartitionSpec(BaseModel):
    spec_id: Optional[int] = Field(None, alias='spec-id')
    fields: List[PartitionField]


class SortDirection(Enum):
    asc = 'asc'
    desc = 'desc'


class NullOrder(Enum):
    nulls_first = 'nulls-first'
    nulls_last = 'nulls-last'


class SortField(BaseModel):
    source_id: int = Field(..., alias='source-id')
    transform: Transform
    direction: SortDirection
    null_order: NullOrder = Field(..., alias='null-order')


class SortOrder(BaseModel):
    order_id: int = Field(..., alias='order-id')
    fields: List[SortField]


class Operation(Enum):
    append = 'append'
    replace = 'replace'
    overwrite = 'overwrite'
    delete = 'delete'


class Summary(BaseModel):
    operation: Operation
    additionalProperties: Optional[str] = None


class Snapshot(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    parent_snapshot_id: Optional[int] = Field(None, alias='parent-snapshot-id')
    sequence_number: Optional[int] = Field(None, alias='sequence-number')
    timestamp_ms: int = Field(..., alias='timestamp-ms')
    manifest_list: str = Field(
        ...,
        alias='manifest-list',
        description="Location of the snapshot's manifest list file",
    )
    summary: Summary
    schema_id: Optional[int] = Field(None, alias='schema-id')


class Type5(Enum):
    tag = 'tag'
    branch = 'branch'


class SnapshotReference(BaseModel):
    type: Type5
    snapshot_id: int = Field(..., alias='snapshot-id')
    max_ref_age_ms: Optional[int] = Field(None, alias='max-ref-age-ms')
    max_snapshot_age_ms: Optional[int] = Field(None, alias='max-snapshot-age-ms')
    min_snapshots_to_keep: Optional[int] = Field(None, alias='min-snapshots-to-keep')


class SnapshotReferences(BaseModel):
    __root__: Optional[Dict[str, SnapshotReference]] = None


class SnapshotLogItem(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    timestamp_ms: int = Field(..., alias='timestamp-ms')


class SnapshotLog(BaseModel):
    __root__: List[SnapshotLogItem]


class MetadataLogItem(BaseModel):
    metadata_file: str = Field(..., alias='metadata-file')
    timestamp_ms: int = Field(..., alias='timestamp-ms')


class MetadataLog(BaseModel):
    __root__: List[MetadataLogItem]


class Action(Enum):
    upgrade_format_version = 'upgrade-format-version'
    add_schema = 'add-schema'
    set_current_schema = 'set-current-schema'
    add_spec = 'add-spec'
    set_default_spec = 'set-default-spec'
    add_sort_order = 'add-sort-order'
    set_default_sort_order = 'set-default-sort-order'
    add_snapshot = 'add-snapshot'
    set_snapshot_ref = 'set-snapshot-ref'
    remove_snapshots = 'remove-snapshots'
    remove_snapshot_ref = 'remove-snapshot-ref'
    set_location = 'set-location'
    set_properties = 'set-properties'
    remove_properties = 'remove-properties'


class BaseUpdate(BaseModel):
    action: Action


class UpgradeFormatVersionUpdate(BaseUpdate):
    format_version: int = Field(..., alias='format-version')


class SetCurrentSchemaUpdate(BaseUpdate):
    schema_id: int = Field(
        ...,
        alias='schema-id',
        description='Schema ID to set as current, or -1 to set last added schema',
    )


class AddPartitionSpecUpdate(BaseUpdate):
    spec: PartitionSpec


class SetDefaultSpecUpdate(BaseUpdate):
    spec_id: int = Field(
        ...,
        alias='spec-id',
        description='Partition spec ID to set as the default, or -1 to set last added spec',
    )


class AddSortOrderUpdate(BaseUpdate):
    sort_order: SortOrder = Field(..., alias='sort-order')


class SetDefaultSortOrderUpdate(BaseUpdate):
    sort_order_id: int = Field(
        ...,
        alias='sort-order-id',
        description='Sort order ID to set as the default, or -1 to set last added sort order',
    )


class AddSnapshotUpdate(BaseUpdate):
    snapshot: Snapshot


class SetSnapshotRefUpdate(BaseUpdate, SnapshotReference):
    ref_name: str = Field(..., alias='ref-name')


class RemoveSnapshotsUpdate(BaseUpdate):
    snapshot_ids: List[int] = Field(..., alias='snapshot-ids')


class RemoveSnapshotRefUpdate(BaseUpdate):
    ref_name: str = Field(..., alias='ref-name')


class SetLocationUpdate(BaseUpdate):
    location: str


class SetPropertiesUpdate(BaseUpdate):
    updates: Dict[str, str]


class RemovePropertiesUpdate(BaseUpdate):
    removals: List[str]


class Type6(Enum):
    assert_create = 'assert-create'
    assert_table_uuid = 'assert-table-uuid'
    assert_ref_snapshot_id = 'assert-ref-snapshot-id'
    assert_last_assigned_field_id = 'assert-last-assigned-field-id'
    assert_current_schema_id = 'assert-current-schema-id'
    assert_last_assigned_partition_id = 'assert-last-assigned-partition-id'
    assert_default_spec_id = 'assert-default-spec-id'
    assert_default_sort_order_id = 'assert-default-sort-order-id'


class TableRequirement(BaseModel):
    """
    Assertions from the client that must be valid for the commit to succeed. Assertions are identified by `type` -
    - `assert-create` - the table must not already exist; used for create transactions
    - `assert-table-uuid` - the table UUID must match the requirement's `uuid`
    - `assert-ref-snapshot-id` - the table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`; if `snapshot-id` is `null` or missing, the ref must not already exist
    - `assert-last-assigned-field-id` - the table's last assigned column id must match the requirement's `last-assigned-field-id`
    - `assert-current-schema-id` - the table's current schema id must match the requirement's `current-schema-id`
    - `assert-last-assigned-partition-id` - the table's last assigned partition id must match the requirement's `last-assigned-partition-id`
    - `assert-default-spec-id` - the table's default spec id must match the requirement's `default-spec-id`
    - `assert-default-sort-order-id` - the table's default sort order id must match the requirement's `default-sort-order-id`
    """

    type: Type6
    ref: Optional[str] = None
    uuid: Optional[str] = None
    snapshot_id: Optional[int] = Field(None, alias='snapshot-id')
    last_assigned_field_id: Optional[int] = Field(None, alias='last-assigned-field-id')
    current_schema_id: Optional[int] = Field(None, alias='current-schema-id')
    last_assigned_partition_id: Optional[int] = Field(
        None, alias='last-assigned-partition-id'
    )
    default_spec_id: Optional[int] = Field(None, alias='default-spec-id')
    default_sort_order_id: Optional[int] = Field(None, alias='default-sort-order-id')


class TokenType(Enum):
    """
    Token type identifier, from RFC 8693 Section 3

    See https://datatracker.ietf.org/doc/html/rfc8693#section-3
    """

    urn_ietf_params_oauth_token_type_access_token = (
        'urn:ietf:params:oauth:token-type:access_token'
    )
    urn_ietf_params_oauth_token_type_refresh_token = (
        'urn:ietf:params:oauth:token-type:refresh_token'
    )
    urn_ietf_params_oauth_token_type_id_token = (
        'urn:ietf:params:oauth:token-type:id_token'
    )
    urn_ietf_params_oauth_token_type_saml1 = 'urn:ietf:params:oauth:token-type:saml1'
    urn_ietf_params_oauth_token_type_saml2 = 'urn:ietf:params:oauth:token-type:saml2'
    urn_ietf_params_oauth_token_type_jwt = 'urn:ietf:params:oauth:token-type:jwt'


class GrantType(Enum):
    client_credentials = 'client_credentials'


class OAuthClientCredentialsRequest(BaseModel):
    """
    OAuth2 client credentials request

    See https://datatracker.ietf.org/doc/html/rfc6749#section-4.4
    """

    grant_type: GrantType
    scope: Optional[str] = None
    client_id: str = Field(
        ...,
        description='Client ID\n\nThis can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.',
    )
    client_secret: str = Field(
        ...,
        description='Client secret\n\nThis can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.',
    )


class GrantType1(Enum):
    urn_ietf_params_oauth_grant_type_token_exchange = (
        'urn:ietf:params:oauth:grant-type:token-exchange'
    )


class OAuthTokenExchangeRequest(BaseModel):
    """
    OAuth2 token exchange request

    See https://datatracker.ietf.org/doc/html/rfc8693
    """

    grant_type: GrantType1
    scope: Optional[str] = None
    requested_token_type: Optional[TokenType] = None
    subject_token: str = Field(
        ..., description='Subject token for token exchange request'
    )
    subject_token_type: TokenType
    actor_token: Optional[str] = Field(
        None, description='Actor token for token exchange request'
    )
    actor_token_type: Optional[TokenType] = None


class OAuthTokenRequest(BaseModel):
    __root__: Union[OAuthClientCredentialsRequest, OAuthTokenExchangeRequest]


class CounterResult(BaseModel):
    unit: str
    value: int


class TimerResult(BaseModel):
    time_unit: str = Field(..., alias='time-unit')
    count: int
    total_duration: int = Field(..., alias='total-duration')


class MetricResult(BaseModel):
    __root__: Union[CounterResult, TimerResult]


class Metrics(BaseModel):
    __root__: Optional[Dict[str, MetricResult]] = None


class CommitReport(BaseModel):
    table_name: str = Field(..., alias='table-name')
    snapshot_id: int = Field(..., alias='snapshot-id')
    sequence_number: int = Field(..., alias='sequence-number')
    operation: str
    metrics: Metrics
    metadata: Optional[Dict[str, str]] = None


class Error(Enum):
    invalid_request = 'invalid_request'
    invalid_client = 'invalid_client'
    invalid_grant = 'invalid_grant'
    unauthorized_client = 'unauthorized_client'
    unsupported_grant_type = 'unsupported_grant_type'
    invalid_scope = 'invalid_scope'


class OAuthError(BaseModel):
    error: Error
    error_description: Optional[str] = None
    error_uri: Optional[str] = None


class TokenType1(Enum):
    """
    Access token type for client credentials or token exchange

    See https://datatracker.ietf.org/doc/html/rfc6749#section-7.1
    """

    bearer = 'bearer'
    mac = 'mac'
    N_A = 'N_A'


class OAuthTokenResponse(BaseModel):
    access_token: str = Field(
        ..., description='The access token, for client credentials or token exchange'
    )
    token_type: TokenType1 = Field(
        ...,
        description='Access token type for client credentials or token exchange\n\nSee https://datatracker.ietf.org/doc/html/rfc6749#section-7.1',
    )
    expires_in: Optional[int] = Field(
        None,
        description='Lifetime of the access token in seconds for client credentials or token exchange',
    )
    issued_token_type: Optional[TokenType] = None
    refresh_token: Optional[str] = Field(
        None, description='Refresh token for client credentials or token exchange'
    )
    scope: Optional[str] = Field(
        None, description='Authorization scope for client credentials or token exchange'
    )


class IcebergErrorResponse(BaseModel):
    class Config:
        extra = Extra.forbid

    error: Optional[ErrorModel] = None


class CreateNamespaceResponse(BaseModel):
    namespace: Namespace
    properties: Optional[Dict[str, str]] = Field(
        {},
        description='Properties stored on the namespace, if supported by the server.',
        example={'owner': 'Ralph', 'created_at': '1452120468'},
    )


class GetNamespaceResponse(BaseModel):
    namespace: Namespace
    properties: Optional[Dict[str, str]] = Field(
        {},
        description='Properties stored on the namespace, if supported by the server. If the server does not support namespace properties, it should return null for this field. If namespace properties are supported, but none are set, it should return an empty object.',
        example={'owner': 'Ralph', 'transient_lastDdlTime': '1452120468'},
    )


class ListTablesResponse(BaseModel):
    identifiers: Optional[List[TableIdentifier]] = Field(None, unique_items=True)


class ListNamespacesResponse(BaseModel):
    namespaces: Optional[List[Namespace]] = Field(None, unique_items=True)


class UpdateNamespacePropertiesResponse(BaseModel):
    updated: List[str] = Field(
        ...,
        description='List of property keys that were added or updated',
        unique_items=True,
    )
    removed: List[str] = Field(..., description='List of properties that were removed')
    missing: Optional[List[str]] = Field(
        None,
        description="List of properties requested for removal that were not found in the namespace's properties. Represents a partial success response. Server's do not need to implement this.",
    )


class CreateNamespaceRequest(BaseModel):
    namespace: Namespace
    properties: Optional[Dict[str, str]] = Field(
        {},
        description='Configured string to string map of properties for the namespace',
        example={'owner': 'Hank Bendickson'},
    )


class RenameTableRequest(BaseModel):
    source: TableIdentifier
    destination: TableIdentifier


class TransformTerm(BaseModel):
    type: Type4
    transform: Transform
    term: Reference


class ReportMetricsRequest1(CommitReport):
    report_type: str = Field(..., alias='report-type')


class Term(BaseModel):
    __root__: Union[Reference, TransformTerm]


class UnaryExpression(BaseModel):
    type: ExpressionType
    term: Term
    value: Dict[str, Any]


class LiteralExpression(BaseModel):
    type: ExpressionType
    term: Term
    value: Dict[str, Any]


class SetExpression(BaseModel):
    type: ExpressionType
    term: Term
    values: List[Dict[str, Any]]


class StructField(BaseModel):
    id: int
    name: str
    type: Type
    required: bool
    doc: Optional[str] = None


class StructType(BaseModel):
    type: Type1
    fields: List[StructField]


class ListType(BaseModel):
    type: Type2
    element_id: int = Field(..., alias='element-id')
    element: Type
    element_required: bool = Field(..., alias='element-required')


class MapType(BaseModel):
    type: Type3
    key_id: int = Field(..., alias='key-id')
    key: Type
    value_id: int = Field(..., alias='value-id')
    value: Type
    value_required: bool = Field(..., alias='value-required')


class Type(BaseModel):
    __root__: Union[PrimitiveType, StructType, ListType, MapType]


class Expression(BaseModel):
    __root__: Union[
        AndOrExpression,
        NotExpression,
        SetExpression,
        LiteralExpression,
        UnaryExpression,
    ]


class AndOrExpression(BaseModel):
    type: ExpressionType
    left: Expression
    right: Expression


class NotExpression(BaseModel):
    type: ExpressionType
    child: Expression


class TableMetadata(BaseModel):
    format_version: int = Field(..., alias='format-version', ge=1, le=2)
    table_uuid: str = Field(..., alias='table-uuid')
    location: Optional[str] = None
    last_updated_ms: Optional[int] = Field(None, alias='last-updated-ms')
    properties: Optional[Dict[str, str]] = None
    schemas: Optional[List[Schema]] = None
    current_schema_id: Optional[int] = Field(None, alias='current-schema-id')
    last_column_id: Optional[int] = Field(None, alias='last-column-id')
    partition_specs: Optional[List[PartitionSpec]] = Field(
        None, alias='partition-specs'
    )
    default_spec_id: Optional[int] = Field(None, alias='default-spec-id')
    last_partition_id: Optional[int] = Field(None, alias='last-partition-id')
    sort_orders: Optional[List[SortOrder]] = Field(None, alias='sort-orders')
    default_sort_order_id: Optional[int] = Field(None, alias='default-sort-order-id')
    snapshots: Optional[List[Snapshot]] = None
    refs: Optional[SnapshotReferences] = None
    current_snapshot_id: Optional[int] = Field(None, alias='current-snapshot-id')
    snapshot_log: Optional[SnapshotLog] = Field(None, alias='snapshot-log')
    metadata_log: Optional[MetadataLog] = Field(None, alias='metadata-log')


class AddSchemaUpdate(BaseUpdate):
    schema_: Schema = Field(..., alias='schema')
    last_column_id: Optional[int] = Field(
        None,
        alias='last-column-id',
        description='The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side.',
    )


class TableUpdate(BaseModel):
    __root__: Union[
        UpgradeFormatVersionUpdate,
        AddSchemaUpdate,
        SetCurrentSchemaUpdate,
        AddPartitionSpecUpdate,
        SetDefaultSpecUpdate,
        AddSortOrderUpdate,
        SetDefaultSortOrderUpdate,
        AddSnapshotUpdate,
        SetSnapshotRefUpdate,
        RemoveSnapshotsUpdate,
        RemoveSnapshotRefUpdate,
        SetLocationUpdate,
        SetPropertiesUpdate,
        RemovePropertiesUpdate,
    ]


class LoadTableResult(BaseModel):
    """
    Result used when a table is successfully loaded.


    The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed.
    Clients can check whether metadata has changed by comparing metadata locations after the table has been created.


    The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.


    The following configurations should be respected by clients:

    ## General Configurations

    - `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled

    ## AWS Configurations

    The following configurations should be respected when working with tables stored in AWS S3
     - `client.region`: region to configure client for making requests to AWS
     - `s3.access-key-id`: id for for credentials that provide access to the data in S3
     - `s3.secret-access-key`: secret for credentials that provide access to data in S3
     - `s3.session-token`: if present, this value should be used for as the session token
     - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the `s3-signer-open-api.yaml` specification

    """

    metadata_location: Optional[str] = Field(
        None,
        alias='metadata-location',
        description='May be null if the table is staged as part of a transaction',
    )
    metadata: TableMetadata
    config: Optional[Dict[str, str]] = None


class CommitTableRequest(BaseModel):
    requirements: List[TableRequirement]
    updates: List[TableUpdate]


class CreateTableRequest(BaseModel):
    name: str
    location: Optional[str] = None
    schema_: Schema = Field(..., alias='schema')
    partition_spec: Optional[PartitionSpec] = Field(None, alias='partition-spec')
    write_order: Optional[SortOrder] = Field(None, alias='write-order')
    stage_create: Optional[bool] = Field(None, alias='stage-create')
    properties: Optional[Dict[str, str]] = None


class ReportMetricsRequest2(BaseModel):
    __root__: Union[ReportMetricsRequest, ReportMetricsRequest1]


class ScanReport(BaseModel):
    table_name: str = Field(..., alias='table-name')
    snapshot_id: int = Field(..., alias='snapshot-id')
    filter: Expression
    schema_id: int = Field(..., alias='schema-id')
    projected_field_ids: List[int] = Field(..., alias='projected-field-ids')
    projected_field_names: List[str] = Field(..., alias='projected-field-names')
    metrics: Metrics
    metadata: Optional[Dict[str, str]] = None


class CommitTableResponse(BaseModel):
    metadata_location: str = Field(..., alias='metadata-location')
    metadata: TableMetadata


class Schema(StructType):
    schema_id: Optional[int] = Field(None, alias='schema-id')
    identifier_field_ids: Optional[List[int]] = Field(
        None, alias='identifier-field-ids'
    )


class ReportMetricsRequest(ScanReport):
    report_type: str = Field(..., alias='report-type')


StructField.update_forward_refs()
ListType.update_forward_refs()
MapType.update_forward_refs()
Expression.update_forward_refs()
TableMetadata.update_forward_refs()
AddSchemaUpdate.update_forward_refs()
CreateTableRequest.update_forward_refs()
ReportMetricsRequest2.update_forward_refs()
