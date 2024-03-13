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
from uuid import UUID

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


class PageToken(BaseModel):
    __root__: str = Field(
        ...,
        description='An opaque token which allows clients to make use of pagination for a list API (e.g. ListTables). Clients will initiate the first paginated request by sending an empty `pageToken` e.g. `GET /tables?pageToken` or `GET /tables?pageToken=` signaling to the service that the response should be paginated.\nServers that support pagination will recognize `pageToken` and return a `next-page-token` in response if there are more results available. After the initial request, it is expected that the value of `next-page-token` from the last response is used in the subsequent request. Servers that do not support pagination will ignore `next-page-token` and return all results.',
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
    next_page_token: Optional[PageToken] = Field(None, alias='next-page-token')
    identifiers: Optional[List[TableIdentifier]] = Field(None, unique_items=True)


class ListNamespacesResponse(BaseModel):
    next_page_token: Optional[PageToken] = Field(None, alias='next-page-token')
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
    __root__: bool = Field(..., example=True)


class IntegerTypeValue(BaseModel):
    __root__: int = Field(..., example=42)


class LongTypeValue(BaseModel):
    __root__: int = Field(..., example=9223372036854775807)


class FloatTypeValue(BaseModel):
    __root__: float = Field(..., example=3.14)


class DoubleTypeValue(BaseModel):
    __root__: float = Field(..., example=123.456)


class DecimalTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description="Decimal type values are serialized as strings. Decimals with a positive scale serialize as numeric plain text, while decimals with a negative scale use scientific notation and the exponent will be equal to the negated scale. For instance, a decimal with a positive scale is '123.4500', with zero scale is '2', and with a negative scale is '2E+20'",
        example='123.4500',
    )


class StringTypeValue(BaseModel):
    __root__: str = Field(..., example='hello')


class UUIDTypeValue(BaseModel):
    __root__: UUID = Field(
        ...,
        description='UUID type values are serialized as a 36-character lowercase string in standard UUID format as specified by RFC-4122',
        example='eb26bdb1-a1d8-4aa6-990e-da940875492c',
        max_length=36,
        min_length=36,
        regex='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    )


class DateTypeValue(BaseModel):
    __root__: date = Field(
        ...,
        description="Date type values follow the 'YYYY-MM-DD' ISO-8601 standard date format",
        example='2007-12-03',
    )


class TimeTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description="Time type values follow the 'HH:MM:SS.ssssss' ISO-8601 format with microsecond precision",
        example='22:31:08.123456',
    )


class TimestampTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description="Timestamp type values follow the 'YYYY-MM-DDTHH:MM:SS.ssssss' ISO-8601 format with microsecond precision",
        example='2007-12-03T10:15:30.123456',
    )


class TimestampTzTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description="TimestampTz type values follow the 'YYYY-MM-DDTHH:MM:SS.ssssss+00:00' ISO-8601 format with microsecond precision, and a timezone offset (+00:00 for UTC)",
        example='2007-12-03T10:15:30.123456+00:00',
    )


class TimestampNanoTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description="Timestamp_ns type values follow the 'YYYY-MM-DDTHH:MM:SS.sssssssss' ISO-8601 format with nanosecond precision",
        example='2007-12-03T10:15:30.123456789',
    )


class TimestampTzNanoTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description="Timestamp_ns type values follow the 'YYYY-MM-DDTHH:MM:SS.sssssssss+00:00' ISO-8601 format with nanosecond precision, and a timezone offset (+00:00 for UTC)",
        example='2007-12-03T10:15:30.123456789+00:00',
    )


class FixedTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description='Fixed length type values are stored and serialized as an uppercase hexadecimal string preserving the fixed length',
        example='78797A',
    )


class BinaryTypeValue(BaseModel):
    __root__: str = Field(
        ...,
        description='Binary type values are stored and serialized as an uppercase hexadecimal string',
        example='78797A',
    )


class CountMap(BaseModel):
    keys: Optional[List[IntegerTypeValue]] = Field(
        None, description='List of integer column ids for each corresponding value'
    )
    values: Optional[List[LongTypeValue]] = Field(
        None, description="List of Long values, matched to 'keys' by index"
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
        TimestampTzTypeValue,
        TimestampNanoTypeValue,
        TimestampTzNanoTypeValue,
        FixedTypeValue,
        BinaryTypeValue,
    ]


class FileFormat(BaseModel):
    __root__: Literal['avro', 'orc', 'parquet']


class ContentFile(BaseModel):
    content: str
    file_path: str = Field(..., alias='file-path')
    file_format: FileFormat = Field(..., alias='file-format')
    spec_id: int = Field(..., alias='spec-id')
    partition: Optional[List[PrimitiveTypeValue]] = Field(
        None,
        description='A list of partition field values ordered based on the fields of the partition spec specified by the `spec-id`',
        example=[1, 'bar'],
    )
    file_size_in_bytes: int = Field(
        ..., alias='file-size-in-bytes', description='Total file size in bytes'
    )
    record_count: int = Field(
        ..., alias='record-count', description='Number of records in the file'
    )
    key_metadata: Optional[BinaryTypeValue] = Field(
        None, alias='key-metadata', description='Encryption key metadata blob'
    )
    split_offsets: Optional[List[int]] = Field(
        None, alias='split-offsets', description='List of splittable offsets'
    )
    sort_order_id: Optional[int] = Field(None, alias='sort-order-id')


class PositionDeleteFile(ContentFile):
    content: Literal['position-deletes']


class EqualityDeleteFile(ContentFile):
    content: Literal['equality-deletes']
    equality_ids: Optional[List[int]] = Field(
        None, alias='equality-ids', description='List of equality field IDs'
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


class ValueMap(BaseModel):
    keys: Optional[List[IntegerTypeValue]] = Field(
        None, description='List of integer column ids for each corresponding value'
    )
    values: Optional[List[PrimitiveTypeValue]] = Field(
        None, description="List of primitive type values, matched to 'keys' by index"
    )


class DataFile(ContentFile):
    content: Literal['data']
    column_sizes: Optional[CountMap] = Field(
        None,
        alias='column-sizes',
        description='Map of column id to total count, including null and NaN',
    )
    value_counts: Optional[CountMap] = Field(
        None, alias='value-counts', description='Map of column id to null value count'
    )
    null_value_counts: Optional[CountMap] = Field(
        None,
        alias='null-value-counts',
        description='Map of column id to null value count',
    )
    nan_value_counts: Optional[CountMap] = Field(
        None,
        alias='nan-value-counts',
        description='Map of column id to number of NaN values in the column',
    )
    lower_bounds: Optional[ValueMap] = Field(
        None,
        alias='lower-bounds',
        description='Map of column id to lower bound primitive type values',
    )
    upper_bounds: Optional[ValueMap] = Field(
        None,
        alias='upper-bounds',
        description='Map of column id to upper bound primitive type values',
    )


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
CreateTableRequest.update_forward_refs()
CreateViewRequest.update_forward_refs()
ReportMetricsRequest.update_forward_refs()
