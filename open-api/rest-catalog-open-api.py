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

from datetime import date
from typing import Any, Dict, List, Literal, Optional, Union

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


class SortDirection(BaseModel):
    __root__: Literal['asc', 'desc']


class NullOrder(BaseModel):
    __root__: Literal['nulls-first', 'nulls-last']


class SortField(BaseModel):
    source_id: int = Field(..., alias='source-id')
    transform: Transform
    direction: SortDirection
    null_order: NullOrder = Field(..., alias='null-order')


class SortOrder(BaseModel):
    order_id: int = Field(..., alias='order-id')
    fields: List[SortField]


class Summary(BaseModel):
    operation: Literal['append', 'replace', 'overwrite', 'delete']
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


class SnapshotReference(BaseModel):
    type: Literal['tag', 'branch']
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


class SQLViewRepresentation(BaseModel):
    type: str
    sql: str
    dialect: str


class ViewRepresentation(BaseModel):
    __root__: SQLViewRepresentation


class ViewHistoryEntry(BaseModel):
    version_id: int = Field(..., alias='version-id')
    timestamp_ms: int = Field(..., alias='timestamp-ms')


class ViewVersion(BaseModel):
    version_id: int = Field(..., alias='version-id')
    timestamp_ms: int = Field(..., alias='timestamp-ms')
    schema_id: int = Field(
        ...,
        alias='schema-id',
        description='Schema ID to set as current, or -1 to set last added schema',
    )
    summary: Dict[str, str]
    representations: List[ViewRepresentation]
    default_catalog: Optional[str] = Field(None, alias='default-catalog')
    default_namespace: Namespace = Field(..., alias='default-namespace')


class BaseUpdate(BaseModel):
    action: str


class AssignUUIDUpdate(BaseUpdate):
    """
    Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned
    """

    action: Literal['assign-uuid']
    uuid: str


class UpgradeFormatVersionUpdate(BaseUpdate):
    action: Literal['upgrade-format-version']
    format_version: int = Field(..., alias='format-version')


class SetCurrentSchemaUpdate(BaseUpdate):
    action: Literal['set-current-schema']
    schema_id: int = Field(
        ...,
        alias='schema-id',
        description='Schema ID to set as current, or -1 to set last added schema',
    )


class AddPartitionSpecUpdate(BaseUpdate):
    action: Literal['add-spec']
    spec: PartitionSpec


class SetDefaultSpecUpdate(BaseUpdate):
    action: Literal['set-default-spec']
    spec_id: int = Field(
        ...,
        alias='spec-id',
        description='Partition spec ID to set as the default, or -1 to set last added spec',
    )


class AddSortOrderUpdate(BaseUpdate):
    action: Literal['add-sort-order']
    sort_order: SortOrder = Field(..., alias='sort-order')


class SetDefaultSortOrderUpdate(BaseUpdate):
    action: Literal['set-default-sort-order']
    sort_order_id: int = Field(
        ...,
        alias='sort-order-id',
        description='Sort order ID to set as the default, or -1 to set last added sort order',
    )


class AddSnapshotUpdate(BaseUpdate):
    action: Literal['add-snapshot']
    snapshot: Snapshot


class SetSnapshotRefUpdate(BaseUpdate, SnapshotReference):
    action: Literal['set-snapshot-ref']
    ref_name: str = Field(..., alias='ref-name')


class RemoveSnapshotsUpdate(BaseUpdate):
    action: Literal['remove-snapshots']
    snapshot_ids: List[int] = Field(..., alias='snapshot-ids')


class RemoveSnapshotRefUpdate(BaseUpdate):
    action: Literal['remove-snapshot-ref']
    ref_name: str = Field(..., alias='ref-name')


class SetLocationUpdate(BaseUpdate):
    action: Literal['set-location']
    location: str


class SetPropertiesUpdate(BaseUpdate):
    action: Literal['set-properties']
    updates: Dict[str, str]


class RemovePropertiesUpdate(BaseUpdate):
    action: Literal['remove-properties']
    removals: List[str]


class AddViewVersionUpdate(BaseUpdate):
    action: Literal['add-view-version']
    view_version: ViewVersion = Field(..., alias='view-version')


class SetCurrentViewVersionUpdate(BaseUpdate):
    action: Literal['set-current-view-version']
    view_version_id: int = Field(
        ...,
        alias='view-version-id',
        description='The view version id to set as current, or -1 to set last added view version id',
    )


class RemoveStatisticsUpdate(BaseUpdate):
    action: Literal['remove-statistics']
    snapshot_id: int = Field(..., alias='snapshot-id')


class RemovePartitionStatisticsUpdate(BaseUpdate):
    action: Literal['remove-partition-statistics']
    snapshot_id: int = Field(..., alias='snapshot-id')


class TableRequirement(BaseModel):
    type: str


class AssertCreate(TableRequirement):
    """
    The table must not already exist; used for create transactions
    """

    type: Literal['assert-create']


class AssertTableUUID(TableRequirement):
    """
    The table UUID must match the requirement's `uuid`
    """

    type: Literal['assert-table-uuid']
    uuid: str


class AssertRefSnapshotId(TableRequirement):
    """
    The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`; if `snapshot-id` is `null` or missing, the ref must not already exist
    """

    type: Literal['assert-ref-snapshot-id']
    ref: str
    snapshot_id: int = Field(..., alias='snapshot-id')


class AssertLastAssignedFieldId(TableRequirement):
    """
    The table's last assigned column id must match the requirement's `last-assigned-field-id`
    """

    type: Literal['assert-last-assigned-field-id']
    last_assigned_field_id: int = Field(..., alias='last-assigned-field-id')


class AssertCurrentSchemaId(TableRequirement):
    """
    The table's current schema id must match the requirement's `current-schema-id`
    """

    type: Literal['assert-current-schema-id']
    current_schema_id: int = Field(..., alias='current-schema-id')


class AssertLastAssignedPartitionId(TableRequirement):
    """
    The table's last assigned partition id must match the requirement's `last-assigned-partition-id`
    """

    type: Literal['assert-last-assigned-partition-id']
    last_assigned_partition_id: int = Field(..., alias='last-assigned-partition-id')


class AssertDefaultSpecId(TableRequirement):
    """
    The table's default spec id must match the requirement's `default-spec-id`
    """

    type: Literal['assert-default-spec-id']
    default_spec_id: int = Field(..., alias='default-spec-id')


class AssertDefaultSortOrderId(TableRequirement):
    """
    The table's default sort order id must match the requirement's `default-sort-order-id`
    """

    type: Literal['assert-default-sort-order-id']
    default_sort_order_id: int = Field(..., alias='default-sort-order-id')


class ViewRequirement(BaseModel):
    type: str


class AssertViewUUID(ViewRequirement):
    """
    The view UUID must match the requirement's `uuid`
    """

    type: Literal['assert-view-uuid']
    uuid: str


class RegisterTableRequest(BaseModel):
    name: str
    metadata_location: str = Field(..., alias='metadata-location')


class TokenType(BaseModel):
    __root__: Literal[
        'urn:ietf:params:oauth:token-type:access_token',
        'urn:ietf:params:oauth:token-type:refresh_token',
        'urn:ietf:params:oauth:token-type:id_token',
        'urn:ietf:params:oauth:token-type:saml1',
        'urn:ietf:params:oauth:token-type:saml2',
        'urn:ietf:params:oauth:token-type:jwt',
    ] = Field(
        ...,
        description='Token type identifier, from RFC 8693 Section 3\n\nSee https://datatracker.ietf.org/doc/html/rfc8693#section-3',
    )


class OAuthClientCredentialsRequest(BaseModel):
    """
    OAuth2 client credentials request

    See https://datatracker.ietf.org/doc/html/rfc6749#section-4.4
    """

    grant_type: Literal['client_credentials']
    scope: Optional[str] = None
    client_id: str = Field(
        ...,
        description='Client ID\n\nThis can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.',
    )
    client_secret: str = Field(
        ...,
        description='Client secret\n\nThis can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.',
    )


class OAuthTokenExchangeRequest(BaseModel):
    """
    OAuth2 token exchange request

    See https://datatracker.ietf.org/doc/html/rfc8693
    """

    grant_type: Literal['urn:ietf:params:oauth:grant-type:token-exchange']
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


class OAuthError(BaseModel):
    error: Literal[
        'invalid_request',
        'invalid_client',
        'invalid_grant',
        'unauthorized_client',
        'unsupported_grant_type',
        'invalid_scope',
    ]
    error_description: Optional[str] = None
    error_uri: Optional[str] = None


class OAuthTokenResponse(BaseModel):
    access_token: str = Field(
        ..., description='The access token, for client credentials or token exchange'
    )
    token_type: Literal['bearer', 'mac', 'N_A'] = Field(
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
    """
    JSON wrapper for all error responses (non-2xx)
    """

    class Config:
        extra = Extra.forbid

    error: ErrorModel


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


class BlobMetadata(BaseModel):
    type: str
    snapshot_id: int = Field(..., alias='snapshot-id')
    sequence_number: int = Field(..., alias='sequence-number')
    fields: List[int]
    properties: Optional[Dict[str, Any]] = None


class PartitionStatisticsFile(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    statistics_path: str = Field(..., alias='statistics-path')
    file_size_in_bytes: int = Field(..., alias='file-size-in-bytes')


class BooleanTypeValue(BaseModel):
    __root__: bool


class IntegerTypeValue(BaseModel):
    __root__: int


class LongTypeValue(BaseModel):
    __root__: int


class FloatTypeValue(BaseModel):
    __root__: float


class DoubleTypeValue(BaseModel):
    __root__: float


class DecimalTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description='Decimal types are serialized as a string to preserve exact numeric precision',
    )


class StringTypeValue(BaseModel):
    __root__: str


class UUIDTypeValue(BaseModel):
    __root__: str = Field(..., description='UUID types are serialized as a string')


class DateTypeValue(BaseModel):
    __root__: date = Field(
        ...,
        description='Date types are serialized from an integer representing days since the Unix epoch to an ISO 8601 date string',
    )


class TimeTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description='Time types are serialized from a long representing microseconds since midnight to an ISO 8601 time string',
    )


class TimestampTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description='Timestamp types are serialized from a long representing microseconds since the Unix epoch to an ISO 8601 timestamp string. During serialization, the timestamp type is checked to see if it should be converted to UTC. If so, the serialized timestamp string will be formatted to represent UTC by appending `+00:00`',
    )


class FixedTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description='serialized as a base16-encoded string representing binary data',
    )


class BinaryTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description='serialized as a base16-encoded string representing binary data',
    )


class PrimitiveTypeValue(BaseModel):
    __root__: Union[
        BooleanTypeValue,
        IntegerTypeValue,
        LongTypeValue,
        FloatTypeValue,
        DoubleTypeValue,
        DecimalTypeValue,
        StringTypeValue,
        UUIDTypeValue,
        DateTypeValue,
        TimeTypeValue,
        TimestampTypeValue,
        FixedTypeValue,
        BinaryTypeValue,
    ]


class FileContent(BaseModel):
    __root__: Literal['data', 'position-deletes', 'equality-deletes']


class FileFormat(BaseModel):
    __root__: Literal['avro', 'orc', 'parquet']


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
    type: Literal['transform']
    transform: Transform
    term: Reference


class SetPartitionStatisticsUpdate(BaseUpdate):
    action: Literal['set-partition-statistics']
    partition_statistics: PartitionStatisticsFile = Field(
        ..., alias='partition-statistics'
    )


class ReportMetricsRequest2(CommitReport):
    report_type: str = Field(..., alias='report-type')


class StatisticsFile(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    statistics_path: str = Field(..., alias='statistics-path')
    file_size_in_bytes: int = Field(..., alias='file-size-in-bytes')
    file_footer_size_in_bytes: int = Field(..., alias='file-footer-size-in-bytes')
    blob_metadata: List[BlobMetadata] = Field(..., alias='blob-metadata')


class Term(BaseModel):
    __root__: Union[Reference, TransformTerm]


class SetStatisticsUpdate(BaseUpdate):
    action: Literal['set-statistics']
    snapshot_id: int = Field(..., alias='snapshot-id')
    statistics: StatisticsFile


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
    type: Literal['struct']
    fields: List[StructField]


class ListType(BaseModel):
    type: Literal['list']
    element_id: int = Field(..., alias='element-id')
    element: Type
    element_required: bool = Field(..., alias='element-required')


class MapType(BaseModel):
    type: Literal['map']
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
    last_sequence_number: Optional[int] = Field(None, alias='last-sequence-number')
    snapshot_log: Optional[SnapshotLog] = Field(None, alias='snapshot-log')
    metadata_log: Optional[MetadataLog] = Field(None, alias='metadata-log')
    statistics_files: Optional[List[StatisticsFile]] = Field(
        None, alias='statistics-files'
    )
    partition_statistics_files: Optional[List[PartitionStatisticsFile]] = Field(
        None, alias='partition-statistics-files'
    )


class ViewMetadata(BaseModel):
    view_uuid: str = Field(..., alias='view-uuid')
    format_version: int = Field(..., alias='format-version', ge=1, le=1)
    location: str
    current_version_id: int = Field(..., alias='current-version-id')
    versions: List[ViewVersion]
    version_log: List[ViewHistoryEntry] = Field(..., alias='version-log')
    schemas: List[Schema]
    properties: Optional[Dict[str, str]] = None


class AddSchemaUpdate(BaseUpdate):
    action: Literal['add-schema']
    schema_: Schema = Field(..., alias='schema')
    last_column_id: Optional[int] = Field(
        None,
        alias='last-column-id',
        description='The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side.',
    )


class AppendDataFileUpdate(BaseUpdate):
    action: Literal['append-data-files']
    data_files: List[ContentFile] = Field(
        ...,
        alias='data-files',
        description='List of data files to be appended to a table',
    )
    schema_: Schema = Field(..., alias='schema')
    spec: PartitionSpec


class TableUpdate(BaseModel):
    __root__: Union[
        AssignUUIDUpdate,
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
        SetStatisticsUpdate,
        RemoveStatisticsUpdate,
        AppendDataFileUpdate,
    ]


class ViewUpdate(BaseModel):
    __root__: Union[
        AssignUUIDUpdate,
        UpgradeFormatVersionUpdate,
        AddSchemaUpdate,
        SetLocationUpdate,
        SetPropertiesUpdate,
        RemovePropertiesUpdate,
        AddViewVersionUpdate,
        SetCurrentViewVersionUpdate,
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
    identifier: Optional[TableIdentifier] = Field(
        None,
        description='Table identifier to update; must be present for CommitTransactionRequest',
    )
    requirements: List[TableRequirement]
    updates: List[TableUpdate]


class CommitViewRequest(BaseModel):
    identifier: Optional[TableIdentifier] = Field(
        None, description='View identifier to update'
    )
    requirements: Optional[List[ViewRequirement]] = None
    updates: List[ViewUpdate]


class CommitTransactionRequest(BaseModel):
    table_changes: List[CommitTableRequest] = Field(..., alias='table-changes')


class CreateTableRequest(BaseModel):
    name: str
    location: Optional[str] = None
    schema_: Schema = Field(..., alias='schema')
    partition_spec: Optional[PartitionSpec] = Field(None, alias='partition-spec')
    write_order: Optional[SortOrder] = Field(None, alias='write-order')
    stage_create: Optional[bool] = Field(None, alias='stage-create')
    properties: Optional[Dict[str, str]] = None


class CreateViewRequest(BaseModel):
    name: str
    location: Optional[str] = None
    schema_: Schema = Field(..., alias='schema')
    view_version: ViewVersion = Field(
        ...,
        alias='view-version',
        description='The view version to create, will replace the schema-id sent within the view-version with the id assigned to the provided schema',
    )
    properties: Dict[str, str]


class LoadViewResult(BaseModel):
    """
    Result used when a view is successfully loaded.


    The view metadata JSON is returned in the `metadata` field. The corresponding file location of view metadata is returned in the `metadata-location` field.
    Clients can check whether metadata has changed by comparing metadata locations after the view has been created.

    The `config` map returns view-specific configuration for the view's resources.

    The following configurations should be respected by clients:

    ## General Configurations

    - `token`: Authorization bearer token to use for view requests if OAuth2 security is enabled

    """

    metadata_location: str = Field(..., alias='metadata-location')
    metadata: ViewMetadata
    config: Optional[Dict[str, str]] = None


class ReportMetricsRequest(BaseModel):
    __root__: Union[ReportMetricsRequest1, ReportMetricsRequest2]


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


class MapTypeValue(BaseModel):
    """
    A map structure serialized with keys and values arrays that maintain type
    """

    keys: Optional[List[TypeValue]] = None
    values: Optional[List[TypeValue]] = None


class StructTypeValue(BaseModel):
    """
    Struct type are serialized, where field id are preserved as a string JSON key, and the fields value is serialized based on the defined type, supporting a deep serialization of nested structures
    """

    __root__: Optional[Dict[str, TypeValue]] = None


class ListTypeValue(BaseModel):
    """
    A list of elements, where each element is serialized according to its specific type logic
    """

    __root__: List[TypeValue] = Field(
        ...,
        description='A list of elements, where each element is serialized according to its specific type logic',
    )


class TypeValue(BaseModel):
    __root__: Union[PrimitiveTypeValue, MapTypeValue, StructTypeValue, ListTypeValue]


class ContentFile(BaseModel):
    spec_id: int = Field(..., alias='spec-id')
    content: FileContent
    file_path: str = Field(..., alias='file-path')
    file_format: FileFormat = Field(..., alias='file-format')
    partition: Optional[StructTypeValue] = None
    file_size_in_bytes: int = Field(..., alias='file-size-in-bytes')
    record_count: int = Field(..., alias='record-count')
    column_sizes: Optional[MapTypeValue] = Field(None, alias='column-sizes')
    value_counts: Optional[MapTypeValue] = Field(None, alias='value-counts')
    null_value_counts: Optional[MapTypeValue] = Field(None, alias='null-value-counts')
    nan_value_counts: Optional[MapTypeValue] = Field(None, alias='nan-value-counts')
    lower_bounds: Optional[MapTypeValue] = Field(None, alias='lower-bounds')
    upper_bounds: Optional[MapTypeValue] = Field(None, alias='upper-bounds')
    key_metadata: Optional[BinaryTypeValue] = Field(None, alias='key-metadata')
    split_offsets: Optional[List[int]] = Field(None, alias='split-offsets')
    equality_ids: Optional[List[int]] = Field(None, alias='equality-ids')
    sort_order_id: Optional[int] = Field(None, alias='sort-order-id')


class Schema(StructType):
    schema_id: Optional[int] = Field(None, alias='schema-id')
    identifier_field_ids: Optional[List[int]] = Field(
        None, alias='identifier-field-ids'
    )


class ReportMetricsRequest1(ScanReport):
    report_type: str = Field(..., alias='report-type')


StructField.update_forward_refs()
ListType.update_forward_refs()
MapType.update_forward_refs()
Expression.update_forward_refs()
TableMetadata.update_forward_refs()
ViewMetadata.update_forward_refs()
AddSchemaUpdate.update_forward_refs()
AppendDataFileUpdate.update_forward_refs()
CreateTableRequest.update_forward_refs()
CreateViewRequest.update_forward_refs()
ReportMetricsRequest.update_forward_refs()
MapTypeValue.update_forward_refs()
StructTypeValue.update_forward_refs()
ListTypeValue.update_forward_refs()
