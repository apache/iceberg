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

from datetime import date, timedelta
from typing import Literal
from uuid import UUID

from pydantic import Base64Str, BaseModel, ConfigDict, Field, RootModel


class ErrorModel(BaseModel):
    """
    JSON error payload returned in a response with further details on the error
    """

    message: str = Field(..., description='Human-readable error message')
    type: str = Field(
        ...,
        description='Internal type definition of the error',
        examples=['NoSuchNamespaceException'],
    )
    code: int = Field(
        ..., description='HTTP response code', examples=[404], ge=400, le=600
    )
    stack: list[str] | None = None


class CatalogConfig(BaseModel):
    """
    Server-provided configuration for the catalog.
    """

    overrides: dict[str, str] = Field(
        ...,
        description='Properties that should be used to override client configuration; applied after defaults and client configuration.',
    )
    defaults: dict[str, str] = Field(
        ...,
        description='Properties that should be used as default configuration; applied before client configuration.',
    )
    endpoints: list[str] | None = Field(
        None,
        description='A list of endpoints that the server supports. The format of each endpoint must be "<HTTP verb> <resource path from OpenAPI REST spec>". The HTTP verb and the resource path must be separated by a space character.',
        examples=[
            [
                'GET /v1/{prefix}/namespaces/{namespace}',
                'GET /v1/{prefix}/namespaces',
                'POST /v1/{prefix}/namespaces',
                'GET /v1/{prefix}/namespaces/{namespace}/tables/{table}',
                'GET /v1/{prefix}/namespaces/{namespace}/views/{view}',
            ]
        ],
    )
    idempotency_key_lifetime: timedelta | None = Field(
        None,
        alias='idempotency-key-lifetime',
        description='Client reuse window for an Idempotency-Key (ISO-8601 duration, e.g., PT30M, PT24H). Interpreted as the maximum time from the first submission using a key to the last retry during which a client may reuse that key. Servers SHOULD accept retries for at least this duration and MAY include a grace period to account for delays/clock skew. Clients SHOULD NOT reuse an Idempotency-Key after this window elapses; they SHOULD generate a new key for any subsequent attempt. Presence of this field indicates the server supports Idempotency-Key semantics for mutation endpoints. If absent, clients MUST assume idempotency is not supported.',
        examples=['PT30M'],
    )


class UpdateNamespacePropertiesRequest(BaseModel):
    removals: list[str] | None = Field(None, examples=[['department', 'access_group']])
    updates: dict[str, str] | None = Field(
        None, examples=[{'owner': 'Hank Bendickson'}]
    )


class Namespace(RootModel[list[str]]):
    """
    Reference to one or more levels of a namespace
    """

    root: list[str] = Field(
        ...,
        description='Reference to one or more levels of a namespace',
        examples=[['accounting', 'tax']],
    )


class PageToken(RootModel[str | None]):
    root: str | None = Field(
        None,
        description='An opaque token that allows clients to make use of pagination for list APIs (e.g. ListTables). Clients may initiate the first paginated request by sending an empty query parameter `pageToken` to the server.\nServers that support pagination should identify the `pageToken` parameter and return a `next-page-token` in the response if there are more results available.  After the initial request, the value of `next-page-token` from each response must be used as the `pageToken` parameter value for the next request. The server must return `null` value for the `next-page-token` in the last response.\nServers that support pagination must return all results in a single response with the value of `next-page-token` set to `null` if the query parameter `pageToken` is not set in the request.\nServers that do not support pagination should ignore the `pageToken` parameter and return all results in a single response. The `next-page-token` must be omitted from the response.\nClients must interpret either `null` or missing response value of `next-page-token` as the end of the listing results.',
    )


class TableIdentifier(BaseModel):
    namespace: Namespace
    name: str


class CatalogObjectIdentifier(RootModel[list[str]]):
    """
    Reference to a catalog object (for example, table, view, or namespace) as an ordered list of hierarchical levels. The object kind is determined by context (e.g. the endpoint or a companion type discriminator), not by the identifier structure alone.
    """

    root: list[str] = Field(
        ...,
        description='Reference to a catalog object (for example, table, view, or namespace) as an ordered list of hierarchical levels. The object kind is determined by context (e.g. the endpoint or a companion type discriminator), not by the identifier structure alone.',
        examples=[['accounting', 'tax', 'paid']],
    )


class PrimitiveType(RootModel[str]):
    root: str = Field(..., examples=[['long', 'string', 'fixed[16]', 'decimal(10,2)']])


class ExpressionType(RootModel[str]):
    root: str = Field(
        ...,
        examples=[
            [
                'true',
                'false',
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
            ]
        ],
    )


class TrueExpression(BaseModel):
    type: Literal['true'] = Field(
        ...,
        examples=[
            [
                'true',
                'false',
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
            ]
        ],
    )


class FalseExpression(BaseModel):
    type: Literal['false'] = Field(
        ...,
        examples=[
            [
                'true',
                'false',
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
            ]
        ],
    )


class Reference(RootModel[str]):
    root: str = Field(..., examples=[['column-name']])


class Transform(RootModel[str]):
    root: str = Field(
        ...,
        examples=[
            ['identity', 'year', 'month', 'day', 'hour', 'bucket[256]', 'truncate[16]']
        ],
    )


class PartitionField(BaseModel):
    field_id: int | None = Field(None, alias='field-id')
    source_id: int = Field(..., alias='source-id')
    name: str
    transform: Transform


class PartitionSpec(BaseModel):
    spec_id: int | None = Field(None, alias='spec-id')
    fields: list[PartitionField]


class SortDirection(RootModel[Literal['asc', 'desc']]):
    root: Literal['asc', 'desc']


class NullOrder(RootModel[Literal['nulls-first', 'nulls-last']]):
    root: Literal['nulls-first', 'nulls-last']


class SortField(BaseModel):
    source_id: int = Field(..., alias='source-id')
    transform: Transform
    direction: SortDirection
    null_order: NullOrder = Field(..., alias='null-order')


class SortOrder(BaseModel):
    order_id: int = Field(..., alias='order-id')
    fields: list[SortField]


class EncryptedKey(BaseModel):
    key_id: str = Field(..., alias='key-id')
    encrypted_key_metadata: Base64Str = Field(
        ...,
        alias='encrypted-key-metadata',
        json_schema_extra={'contentEncoding': 'base64'},
    )
    encrypted_by_id: str | None = Field(None, alias='encrypted-by-id')
    properties: dict[str, str] | None = None


class Summary(BaseModel):
    model_config = ConfigDict(
        extra='allow',
    )
    __pydantic_extra__: dict[str, str]
    operation: Literal['append', 'replace', 'overwrite', 'delete']


class Snapshot(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    parent_snapshot_id: int | None = Field(None, alias='parent-snapshot-id')
    sequence_number: int | None = Field(None, alias='sequence-number')
    timestamp_ms: int = Field(..., alias='timestamp-ms')
    manifest_list: str = Field(
        ...,
        alias='manifest-list',
        description="Location of the snapshot's manifest list file",
    )
    first_row_id: int | None = Field(
        None,
        alias='first-row-id',
        description='The first _row_id assigned to the first row in the first data file in the first manifest',
    )
    added_rows: int | None = Field(
        None,
        alias='added-rows',
        description='The upper bound of the number of rows with assigned row IDs',
    )
    summary: Summary
    schema_id: int | None = Field(None, alias='schema-id')


class SnapshotReference(BaseModel):
    type: Literal['tag', 'branch']
    snapshot_id: int = Field(..., alias='snapshot-id')
    max_ref_age_ms: int | None = Field(None, alias='max-ref-age-ms')
    max_snapshot_age_ms: int | None = Field(None, alias='max-snapshot-age-ms')
    min_snapshots_to_keep: int | None = Field(None, alias='min-snapshots-to-keep')


class SnapshotReferences(RootModel[dict[str, SnapshotReference]]):
    root: dict[str, SnapshotReference]


class SnapshotLogItem(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    timestamp_ms: int = Field(..., alias='timestamp-ms')


class SnapshotLog(RootModel[list[SnapshotLogItem]]):
    root: list[SnapshotLogItem]


class MetadataLogItem(BaseModel):
    metadata_file: str = Field(..., alias='metadata-file')
    timestamp_ms: int = Field(..., alias='timestamp-ms')


class MetadataLog(RootModel[list[MetadataLogItem]]):
    root: list[MetadataLogItem]


class SQLViewRepresentation(BaseModel):
    type: str
    sql: str
    dialect: str


class ViewRepresentation(RootModel[SQLViewRepresentation]):
    root: SQLViewRepresentation


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
    summary: dict[str, str]
    representations: list[ViewRepresentation]
    default_catalog: str | None = Field(None, alias='default-catalog')
    default_namespace: Namespace = Field(..., alias='default-namespace')


class BaseUpdate(BaseModel):
    action: str


class AssignUUIDUpdate(BaseUpdate):
    """
    Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned
    """

    action: Literal['assign-uuid'] = 'assign-uuid'
    uuid: str


class UpgradeFormatVersionUpdate(BaseUpdate):
    action: Literal['upgrade-format-version'] = 'upgrade-format-version'
    format_version: int = Field(..., alias='format-version')


class SetCurrentSchemaUpdate(BaseUpdate):
    action: Literal['set-current-schema'] = 'set-current-schema'
    schema_id: int = Field(
        ...,
        alias='schema-id',
        description='Schema ID to set as current, or -1 to set last added schema',
    )


class AddPartitionSpecUpdate(BaseUpdate):
    action: Literal['add-spec'] = 'add-spec'
    spec: PartitionSpec


class SetDefaultSpecUpdate(BaseUpdate):
    action: Literal['set-default-spec'] = 'set-default-spec'
    spec_id: int = Field(
        ...,
        alias='spec-id',
        description='Partition spec ID to set as the default, or -1 to set last added spec',
    )


class AddSortOrderUpdate(BaseUpdate):
    action: Literal['add-sort-order'] = 'add-sort-order'
    sort_order: SortOrder = Field(..., alias='sort-order')


class SetDefaultSortOrderUpdate(BaseUpdate):
    action: Literal['set-default-sort-order'] = 'set-default-sort-order'
    sort_order_id: int = Field(
        ...,
        alias='sort-order-id',
        description='Sort order ID to set as the default, or -1 to set last added sort order',
    )


class AddSnapshotUpdate(BaseUpdate):
    action: Literal['add-snapshot'] = 'add-snapshot'
    snapshot: Snapshot


class SetSnapshotRefUpdate(BaseUpdate, SnapshotReference):
    action: Literal['set-snapshot-ref'] = 'set-snapshot-ref'
    ref_name: str = Field(..., alias='ref-name')


class RemoveSnapshotsUpdate(BaseUpdate):
    action: Literal['remove-snapshots'] = 'remove-snapshots'
    snapshot_ids: list[int] = Field(..., alias='snapshot-ids')


class RemoveSnapshotRefUpdate(BaseUpdate):
    action: Literal['remove-snapshot-ref'] = 'remove-snapshot-ref'
    ref_name: str = Field(..., alias='ref-name')


class SetLocationUpdate(BaseUpdate):
    action: Literal['set-location'] = 'set-location'
    location: str


class SetPropertiesUpdate(BaseUpdate):
    action: Literal['set-properties'] = 'set-properties'
    updates: dict[str, str]


class RemovePropertiesUpdate(BaseUpdate):
    action: Literal['remove-properties'] = 'remove-properties'
    removals: list[str]


class AddViewVersionUpdate(BaseUpdate):
    action: Literal['add-view-version'] = 'add-view-version'
    view_version: ViewVersion = Field(..., alias='view-version')


class SetCurrentViewVersionUpdate(BaseUpdate):
    action: Literal['set-current-view-version'] = 'set-current-view-version'
    view_version_id: int = Field(
        ...,
        alias='view-version-id',
        description='The view version id to set as current, or -1 to set last added view version id',
    )


class RemoveStatisticsUpdate(BaseUpdate):
    action: Literal['remove-statistics'] = 'remove-statistics'
    snapshot_id: int = Field(..., alias='snapshot-id')


class RemovePartitionStatisticsUpdate(BaseUpdate):
    action: Literal['remove-partition-statistics'] = 'remove-partition-statistics'
    snapshot_id: int = Field(..., alias='snapshot-id')


class RemovePartitionSpecsUpdate(BaseUpdate):
    action: Literal['remove-partition-specs'] = 'remove-partition-specs'
    spec_ids: list[int] = Field(..., alias='spec-ids')


class RemoveSchemasUpdate(BaseUpdate):
    action: Literal['remove-schemas'] = 'remove-schemas'
    schema_ids: list[int] = Field(..., alias='schema-ids')


class AddEncryptionKeyUpdate(BaseUpdate):
    action: Literal['add-encryption-key'] = 'add-encryption-key'
    encryption_key: EncryptedKey = Field(..., alias='encryption-key')


class RemoveEncryptionKeyUpdate(BaseUpdate):
    action: Literal['remove-encryption-key'] = 'remove-encryption-key'
    key_id: str = Field(..., alias='key-id')


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
    The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`.
    The `snapshot-id` field is required in this object, but in the case of a `null`
    the ref must not already exist.

    """

    type: Literal['assert-ref-snapshot-id'] = 'assert-ref-snapshot-id'
    ref: str
    snapshot_id: int = Field(..., alias='snapshot-id')


class AssertLastAssignedFieldId(TableRequirement):
    """
    The table's last assigned column id must match the requirement's `last-assigned-field-id`
    """

    type: Literal['assert-last-assigned-field-id'] = 'assert-last-assigned-field-id'
    last_assigned_field_id: int = Field(..., alias='last-assigned-field-id')


class AssertCurrentSchemaId(TableRequirement):
    """
    The table's current schema id must match the requirement's `current-schema-id`
    """

    type: Literal['assert-current-schema-id'] = 'assert-current-schema-id'
    current_schema_id: int = Field(..., alias='current-schema-id')


class AssertLastAssignedPartitionId(TableRequirement):
    """
    The table's last assigned partition id must match the requirement's `last-assigned-partition-id`
    """

    type: Literal['assert-last-assigned-partition-id'] = (
        'assert-last-assigned-partition-id'
    )
    last_assigned_partition_id: int = Field(..., alias='last-assigned-partition-id')


class AssertDefaultSpecId(TableRequirement):
    """
    The table's default spec id must match the requirement's `default-spec-id`
    """

    type: Literal['assert-default-spec-id'] = 'assert-default-spec-id'
    default_spec_id: int = Field(..., alias='default-spec-id')


class AssertDefaultSortOrderId(TableRequirement):
    """
    The table's default sort order id must match the requirement's `default-sort-order-id`
    """

    type: Literal['assert-default-sort-order-id'] = 'assert-default-sort-order-id'
    default_sort_order_id: int = Field(..., alias='default-sort-order-id')


class AssertViewUUID(BaseModel):
    """
    The view UUID must match the requirement's `uuid`
    """

    type: Literal['assert-view-uuid']
    uuid: str


class StorageCredential(BaseModel):
    prefix: str = Field(
        ...,
        description='Indicates a storage location prefix where the credential is relevant. Clients should choose the most specific prefix (by selecting the longest prefix) if several credentials of the same type are available.',
    )
    config: dict[str, str]


class Action(BaseModel):
    action: str
    field_id: int = Field(
        ..., alias='field-id', description='field id of the column being projected.'
    )


class MaskAlphanum(Action):
    """
    Redacts the column value Unicode code point by code point using the following rules:
    - Digits (U+0030–U+0039, 0-9) are replaced with 'n' - The following punctuation characters are kept as-is:
        U+0028 '('  LEFT PARENTHESIS
        U+0029 ')'  RIGHT PARENTHESIS
        U+002C ','  COMMA
        U+002E '.'  FULL STOP
        U+002D '-'  HYPHEN-MINUS
        U+0040 '@'  COMMERCIAL AT
    - All other Unicode characters (including letters, whitespace, and any punctuation
      not listed above) are replaced with 'x'

    For example: "prashant010696@gmail.com" → "xxxxxxxxnnnnnn@xxxxx.xxx"
    Applicable to: string

    """

    action: Literal['mask-alphanum'] = 'mask-alphanum'


class MaskToDefault(Action):
    """
    Replaces the column value with a predefined type-specific default that conceals the original data while preserving type compatibility. Engines MUST use exactly the values listed below to ensure consistency across implementations.
    Default values by type: - boolean: false - int: 999999999 - long: 999999999 - float: 0.0 - double: 0.0 - decimal(p, s): 0 (zero with s digits after the decimal point, e.g. 0.00 for decimal(p,2)) - string: "XXXXXXXX" - date: 9999-12-31 - time: 00:00:00 - timestamp: 9999-12-31T00:00:00 - timestamptz: 9999-12-31T00:00:00+00:00 - timestamp_ns: 2261-12-31T00:00:00.000000000 - timestamptz_ns: 2261-12-31T00:00:00.000000000+00:00 - uuid: 00000000-0000-0000-0000-000000000000 - fixed(n): n zero bytes - binary: empty byte sequence - variant: {"masked": true} - geometry: POINT EMPTY - geography: POINT EMPTY - list: empty list [] - map: empty map {} - struct: struct with each field set to its type-specific default (applied recursively)
    Note: nanosecond-precision timestamps (timestamp_ns, timestamptz_ns) cannot represent 9999-12-31 because nanoseconds from epoch overflows a 64-bit signed integer past approximately 2262-04-11; 2261-12-31 is used as the closest clean far-future sentinel.
    Applicable to: all data types

    """

    action: Literal['mask-to-default'] = 'mask-to-default'


class ReplaceWithNull(Action):
    """
    Replaces the entire column value with NULL.
    Applicable to: all nullable types

    """

    action: Literal['replace-with-null'] = 'replace-with-null'


class ShowFirst4(Action):
    """
    Preserves the first 4 Unicode code points of the column value and redacts the remainder using mask-alphanum rules (see MaskAlphanum for the exact character rules). Values with 4 or fewer Unicode code points are returned unchanged.
    For example: "prashant010696@gmail.com" → "prasxxxxnnnnnn@xxxxx.xxx"
    Applicable to: string

    """

    action: Literal['show-first-4'] = 'show-first-4'


class ShowLast4(Action):
    """
    Redacts all Unicode code points except the last 4 using mask-alphanum rules (see MaskAlphanum for the exact character rules). Values with 4 or fewer Unicode code points are returned unchanged.
    For example: "4111-1111-1111-4444" → "nnnn-nnnn-nnnn-4444"
    Applicable to: string

    """

    action: Literal['show-last-4'] = 'show-last-4'


class TruncateToYear(Action):
    """
    Truncates the column value to year precision, setting month, day, and time components to their minimum values. The output type matches the input type.
    For example: 2024-07-15 → 2024-01-01 For timestamptz and timestamptz_ns, truncation is performed in UTC.
    Applicable to: date, timestamp, timestamptz, timestamp_ns, timestamptz_ns

    """

    action: Literal['truncate-to-year'] = 'truncate-to-year'


class TruncateToMonth(Action):
    """
    Truncates the column value to year and month precision, setting day and time components to their minimum values. The output type matches the input type.
    For example: 2024-07-15 → 2024-07-01 For timestamptz and timestamptz_ns, truncation is performed in UTC.
    Applicable to: date, timestamp, timestamptz, timestamp_ns, timestamptz_ns

    """

    action: Literal['truncate-to-month'] = 'truncate-to-month'


class Sha256Global(Action):
    """
    Applies SHA-256 as specified in NIST FIPS 180-4. Deterministic across all queries
    and engines — the same input always produces the same output.

    Input-to-bytes encoding by type:
    - string: UTF-8 encoded bytes
    - int: 4 bytes, little-endian
    - long: 8 bytes, little-endian
    - binary: raw bytes as-is

    Output encoding by type:
    - string: 64-character lowercase hexadecimal string
    - int: first 4 bytes of the digest, read as a signed two's complement little-endian int
    - long: first 8 bytes of the digest, read as a signed two's complement little-endian long
    - binary: the full 32-byte raw SHA-256 digest

    Applicable to: string, int, long, binary

    """

    action: Literal['sha-256-global'] = 'sha-256-global'


class Sha256QueryLocal(Action):
    """
    Applies SHA-256 with a per-query random salt, making the output non-deterministic
    across queries while remaining consistent within a single query.

    The engine MUST generate a cryptographically random salt of at least 16 bytes for each query and apply it as:
      SHA-256(salt_bytes || canonical_bytes)
    where canonical_bytes follows the same encoding rules as sha-256-global.

    Output encoding follows the same rules as sha-256-global.

    Applicable to: string, int, long, binary

    """

    action: Literal['sha-256-query-local'] = 'sha-256-query-local'


class LoadCredentialsResponse(BaseModel):
    storage_credentials: list[StorageCredential] = Field(
        ..., alias='storage-credentials'
    )


class AsyncPlanningResult(BaseModel):
    status: Literal['submitted'] = Field(
        ..., description='Status of a server-side planning operation'
    )
    plan_id: str = Field(
        ..., alias='plan-id', description='ID used to track a planning request'
    )


class EmptyPlanningResult(BaseModel):
    """
    Empty server-side planning result
    """

    status: Literal['cancelled'] = Field(
        ..., description='Status of a server-side planning operation'
    )


class PlanStatus(RootModel[Literal['completed', 'submitted', 'cancelled', 'failed']]):
    root: Literal['completed', 'submitted', 'cancelled', 'failed'] = Field(
        ..., description='Status of a server-side planning operation'
    )


class RegisterTableRequest(BaseModel):
    name: str
    metadata_location: str = Field(..., alias='metadata-location')
    overwrite: bool | None = Field(
        False,
        description='Whether to overwrite table metadata if the table already exists',
    )


class RegisterViewRequest(BaseModel):
    name: str
    metadata_location: str = Field(..., alias='metadata-location')


class TokenType(
    RootModel[
        Literal[
            'urn:ietf:params:oauth:token-type:access_token',
            'urn:ietf:params:oauth:token-type:refresh_token',
            'urn:ietf:params:oauth:token-type:id_token',
            'urn:ietf:params:oauth:token-type:saml1',
            'urn:ietf:params:oauth:token-type:saml2',
            'urn:ietf:params:oauth:token-type:jwt',
        ]
    ]
):
    root: Literal[
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
    The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of the endpoint.

    OAuth2 client credentials request

    See https://datatracker.ietf.org/doc/html/rfc6749#section-4.4
    """

    grant_type: Literal['client_credentials']
    scope: str | None = None
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
    The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of the endpoint.

    OAuth2 token exchange request

    See https://datatracker.ietf.org/doc/html/rfc8693
    """

    grant_type: Literal['urn:ietf:params:oauth:grant-type:token-exchange']
    scope: str | None = None
    requested_token_type: TokenType | None = None
    subject_token: str = Field(
        ..., description='Subject token for token exchange request'
    )
    subject_token_type: TokenType
    actor_token: str | None = Field(
        None, description='Actor token for token exchange request'
    )
    actor_token_type: TokenType | None = None


class OAuthTokenRequest(
    RootModel[OAuthClientCredentialsRequest | OAuthTokenExchangeRequest]
):
    root: OAuthClientCredentialsRequest | OAuthTokenExchangeRequest = Field(
        ...,
        deprecated=True,
        description='The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of the endpoint.',
    )


class CounterResult(BaseModel):
    unit: str
    value: int


class TimerResult(BaseModel):
    time_unit: str = Field(..., alias='time-unit')
    count: int
    total_duration: int = Field(..., alias='total-duration')


class MetricResult(RootModel[CounterResult | TimerResult]):
    root: CounterResult | TimerResult


class Metrics(RootModel[dict[str, MetricResult]]):
    root: dict[str, MetricResult]


class CommitReport(BaseModel):
    table_name: str = Field(..., alias='table-name')
    snapshot_id: int = Field(..., alias='snapshot-id')
    sequence_number: int = Field(..., alias='sequence-number')
    operation: str
    metrics: Metrics
    metadata: dict[str, str] | None = None


class OAuthError(BaseModel):
    """
    The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of the endpoint.
    """

    error: Literal[
        'invalid_request',
        'invalid_client',
        'invalid_grant',
        'unauthorized_client',
        'unsupported_grant_type',
        'invalid_scope',
    ]
    error_description: str | None = None
    error_uri: str | None = None


class OAuthTokenResponse(BaseModel):
    """
    The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of the endpoint.
    """

    access_token: str = Field(
        ..., description='The access token, for client credentials or token exchange'
    )
    token_type: Literal['bearer', 'mac', 'N_A'] = Field(
        ...,
        description='Access token type for client credentials or token exchange\n\nSee https://datatracker.ietf.org/doc/html/rfc6749#section-7.1',
    )
    expires_in: int | None = Field(
        None,
        description='Lifetime of the access token in seconds for client credentials or token exchange',
    )
    issued_token_type: TokenType | None = None
    refresh_token: str | None = Field(
        None, description='Refresh token for client credentials or token exchange'
    )
    scope: str | None = Field(
        None, description='Authorization scope for client credentials or token exchange'
    )


class IcebergErrorResponse(BaseModel):
    """
    JSON wrapper for all error responses (non-2xx)
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    error: ErrorModel


class CreateNamespaceResponse(BaseModel):
    namespace: Namespace
    properties: dict[str, str] | None = Field(
        {},
        description='Properties stored on the namespace, if supported by the server.',
        examples=[{'owner': 'Ralph', 'created_at': '1452120468'}],
    )


class GetNamespaceResponse(BaseModel):
    namespace: Namespace
    properties: dict[str, str] | None = Field(
        {},
        description='Properties stored on the namespace, if supported by the server. If the server does not support namespace properties, it should return null for this field. If namespace properties are supported, but none are set, it should return an empty object.',
        examples=[{'owner': 'Ralph', 'transient_lastDdlTime': '1452120468'}],
    )


class ListTablesResponse(BaseModel):
    next_page_token: PageToken | None = Field(None, alias='next-page-token')
    identifiers: list[TableIdentifier] | None = None


class ListFunctionsResponse(BaseModel):
    next_page_token: PageToken | None = Field(None, alias='next-page-token')
    identifiers: list[CatalogObjectIdentifier] | None = None


class ListNamespacesResponse(BaseModel):
    next_page_token: PageToken | None = Field(None, alias='next-page-token')
    namespaces: list[Namespace] | None = None


class FunctionSQLRepresentation(BaseModel):
    type: Literal['sql']
    dialect: str = Field(
        ..., description='SQL dialect identifier (e.g., "spark", "trino").'
    )
    sql: str = Field(..., description='SQL expression text.')


class FunctionDefinitionVersionRef(BaseModel):
    definition_id: str = Field(..., alias='definition-id')
    version_id: int = Field(..., alias='version-id')


class UpdateNamespacePropertiesResponse(BaseModel):
    updated: list[str] = Field(
        ..., description='List of property keys that were added or updated'
    )
    removed: list[str] = Field(..., description='List of properties that were removed')
    missing: list[str] | None = Field(
        None,
        description="List of properties requested for removal that were not found in the namespace's properties. Represents a partial success response. Server's do not need to implement this.",
    )


class BlobMetadata(BaseModel):
    type: str
    snapshot_id: int = Field(..., alias='snapshot-id')
    sequence_number: int = Field(..., alias='sequence-number')
    fields: list[int]
    properties: dict[str, str] | None = None


class PartitionStatisticsFile(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    statistics_path: str = Field(..., alias='statistics-path')
    file_size_in_bytes: int = Field(..., alias='file-size-in-bytes')


class BooleanTypeValue(RootModel[bool]):
    root: bool = Field(..., examples=[True])


class IntegerTypeValue(RootModel[int]):
    root: int = Field(..., examples=[42])


class LongTypeValue(RootModel[int]):
    root: int = Field(..., examples=[9223372036854775807])


class FloatTypeValue(RootModel[float]):
    root: float = Field(..., examples=[3.14])


class DoubleTypeValue(RootModel[float]):
    root: float = Field(..., examples=[123.456])


class DecimalTypeValue(RootModel[str]):
    root: str = Field(
        ...,
        description="Decimal type values are serialized as strings. Decimals with a positive scale serialize as numeric plain text, while decimals with a negative scale use scientific notation and the exponent will be equal to the negated scale. For instance, a decimal with a positive scale is '123.4500', with zero scale is '2', and with a negative scale is '2E+20'",
        examples=['123.4500'],
    )


class StringTypeValue(RootModel[str]):
    root: str = Field(..., examples=['hello'])


class UUIDTypeValue(RootModel[UUID]):
    root: UUID = Field(
        ...,
        description='UUID type values are serialized as a 36-character lowercase string in standard UUID format as specified by RFC-4122',
        examples=['eb26bdb1-a1d8-4aa6-990e-da940875492c'],
        max_length=36,
        min_length=36,
        pattern='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    )


class DateTypeValue(RootModel[date]):
    root: date = Field(
        ...,
        description="Date type values follow the 'YYYY-MM-DD' ISO-8601 standard date format",
        examples=['2007-12-03'],
    )


class TimeTypeValue(RootModel[str]):
    root: str = Field(
        ...,
        description="Time type values follow the 'HH:MM:SS.ssssss' ISO-8601 format with microsecond precision",
        examples=['22:31:08.123456'],
    )


class TimestampTypeValue(RootModel[str]):
    root: str = Field(
        ...,
        description="Timestamp type values follow the 'YYYY-MM-DDTHH:MM:SS.ssssss' ISO-8601 format with microsecond precision",
        examples=['2007-12-03T10:15:30.123456'],
    )


class TimestampTzTypeValue(RootModel[str]):
    root: str = Field(
        ...,
        description="TimestampTz type values follow the 'YYYY-MM-DDTHH:MM:SS.ssssss+00:00' ISO-8601 format with microsecond precision, and a timezone offset (+00:00 for UTC)",
        examples=['2007-12-03T10:15:30.123456+00:00'],
    )


class TimestampNanoTypeValue(RootModel[str]):
    root: str = Field(
        ...,
        description="Timestamp_ns type values follow the 'YYYY-MM-DDTHH:MM:SS.sssssssss' ISO-8601 format with nanosecond precision",
        examples=['2007-12-03T10:15:30.123456789'],
    )


class TimestampTzNanoTypeValue(RootModel[str]):
    root: str = Field(
        ...,
        description="Timestamp_ns type values follow the 'YYYY-MM-DDTHH:MM:SS.sssssssss+00:00' ISO-8601 format with nanosecond precision, and a timezone offset (+00:00 for UTC)",
        examples=['2007-12-03T10:15:30.123456789+00:00'],
    )


class FixedTypeValue(RootModel[str]):
    root: str = Field(
        ...,
        description='Fixed length type values are stored and serialized as an uppercase hexadecimal string preserving the fixed length',
        examples=['78797A'],
    )


class BinaryTypeValue(RootModel[str]):
    root: str = Field(
        ...,
        description='Binary type values are stored and serialized as an uppercase hexadecimal string',
        examples=['78797A'],
    )


class CountMap(BaseModel):
    keys: list[IntegerTypeValue] | None = Field(
        None, description='List of integer column ids for each corresponding value'
    )
    values: list[LongTypeValue] | None = Field(
        None, description="List of Long values, matched to 'keys' by index"
    )


class PrimitiveTypeValue(
    RootModel[
        BooleanTypeValue
        | IntegerTypeValue
        | LongTypeValue
        | FloatTypeValue
        | DoubleTypeValue
        | DecimalTypeValue
        | StringTypeValue
        | UUIDTypeValue
        | DateTypeValue
        | TimeTypeValue
        | TimestampTypeValue
        | TimestampTzTypeValue
        | TimestampNanoTypeValue
        | TimestampTzNanoTypeValue
        | FixedTypeValue
        | BinaryTypeValue
    ]
):
    root: (
        BooleanTypeValue
        | IntegerTypeValue
        | LongTypeValue
        | FloatTypeValue
        | DoubleTypeValue
        | DecimalTypeValue
        | StringTypeValue
        | UUIDTypeValue
        | DateTypeValue
        | TimeTypeValue
        | TimestampTypeValue
        | TimestampTzTypeValue
        | TimestampNanoTypeValue
        | TimestampTzNanoTypeValue
        | FixedTypeValue
        | BinaryTypeValue
    )


class FileFormat(RootModel[Literal['avro', 'orc', 'parquet', 'puffin']]):
    root: Literal['avro', 'orc', 'parquet', 'puffin']


class ContentFile(BaseModel):
    content: str
    file_path: str = Field(..., alias='file-path')
    file_format: FileFormat = Field(..., alias='file-format')
    spec_id: int = Field(..., alias='spec-id')
    partition: list[PrimitiveTypeValue] = Field(
        ...,
        description='A list of partition field values ordered based on the fields of the partition spec specified by the `spec-id`',
        examples=[[1, 'bar']],
    )
    file_size_in_bytes: int = Field(
        ..., alias='file-size-in-bytes', description='Total file size in bytes'
    )
    record_count: int = Field(
        ..., alias='record-count', description='Number of records in the file'
    )
    key_metadata: BinaryTypeValue | None = Field(
        None, alias='key-metadata', description='Encryption key metadata blob'
    )
    split_offsets: list[int] | None = Field(
        None, alias='split-offsets', description='List of splittable offsets'
    )
    sort_order_id: int | None = Field(None, alias='sort-order-id')


class PositionDeleteFile(ContentFile):
    content: Literal['position-deletes']
    content_offset: int | None = Field(
        None,
        alias='content-offset',
        description='Offset within the delete file of delete content',
    )
    content_size_in_bytes: int | None = Field(
        None,
        alias='content-size-in-bytes',
        description='Length, in bytes, of the delete content; required if content-offset is present',
    )


class EqualityDeleteFile(ContentFile):
    content: Literal['equality-deletes']
    equality_ids: list[int] | None = Field(
        None, alias='equality-ids', description='List of equality field IDs'
    )


class FieldName(RootModel[str]):
    root: str = Field(
        ...,
        description='A full field name (including parent field names), such as those passed in APIs like Java `Schema#findField(String name)`.\nThe nested field name follows these rules - Nested struct fields are named by concatenating field names at each struct level using dot (`.`) delimiter, e.g. employer.contact_info.address.zip_code - Nested fields in a map key are named using the keyword `key`, e.g. employee_address_map.key.first_name - Nested fields in a map value are named using the keyword `value`, e.g. employee_address_map.value.zip_code - Nested fields in a list are named using the keyword `element`, e.g. employees.element.first_name',
    )


class PlanTask(RootModel[str]):
    root: str = Field(
        ...,
        description='An opaque string provided by the REST server that represents a unit of work to produce file scan tasks for scan planning. This allows clients to fetch tasks across multiple requests to accommodate large result sets.',
    )


class MultiValuedMap(RootModel[dict[str, list[str]]]):
    """
    A map of string keys where each key can map to multiple string values.
    """

    root: dict[str, list[str]]


class RemoteSignRequest(BaseModel):
    """
    The request to be signed remotely.
    """

    region: str
    uri: str
    method: Literal['PUT', 'GET', 'HEAD', 'POST', 'DELETE', 'PATCH', 'OPTIONS']
    headers: MultiValuedMap
    properties: dict[str, str] | None = None
    body: str | None = Field(
        None,
        description='Optional body of the request to send to the signing API. This should only be populated for requests where the body of the message contains content which must be validated before a request is signed, such as the S3 DeleteObjects call.',
    )
    provider: str | None = Field(
        None,
        description='The storage provider for which the request is to be signed. The provider should correspond to the scheme used for a storage native URI. For example `s3` for AWS S3 paths. For backwards compatibility, if this is not specified, the provider is assumed to be `s3`.',
    )


class RemoteSignResult(BaseModel):
    """
    The result of a remote request signing operation.
    """

    uri: str
    headers: MultiValuedMap


class CreateNamespaceRequest(BaseModel):
    namespace: Namespace
    properties: dict[str, str] | None = Field(
        {},
        description='Configured string to string map of properties for the namespace',
        examples=[{'owner': 'Hank Bendickson'}],
    )


class RenameTableRequest(BaseModel):
    source: TableIdentifier
    destination: TableIdentifier


class TransformTerm(BaseModel):
    type: Literal['transform']
    transform: Transform
    term: Reference


class SetPartitionStatisticsUpdate(BaseUpdate):
    action: Literal['set-partition-statistics'] = 'set-partition-statistics'
    partition_statistics: PartitionStatisticsFile = Field(
        ..., alias='partition-statistics'
    )


class ViewRequirement(RootModel[AssertViewUUID]):
    root: AssertViewUUID = Field(..., discriminator='type')


class FailedPlanningResult(IcebergErrorResponse):
    """
    Failed server-side planning result
    """

    status: Literal['failed'] = Field(
        ..., description='Status of a server-side planning operation'
    )


class ReportMetricsRequest2(CommitReport):
    report_type: str = Field(..., alias='report-type')


class FunctionRepresentation(RootModel[FunctionSQLRepresentation]):
    root: FunctionSQLRepresentation = Field(
        ..., description='UDF implementation representation.'
    )


class FunctionDefinitionLogEntry(BaseModel):
    timestamp_ms: int = Field(
        ...,
        alias='timestamp-ms',
        description='Timestamp when the function was updated to use the definition versions.',
    )
    definition_versions: list[FunctionDefinitionVersionRef] = Field(
        ...,
        alias='definition-versions',
        description='Mapping of each definition to its selected version at this time.',
    )


class StatisticsFile(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    statistics_path: str = Field(..., alias='statistics-path')
    file_size_in_bytes: int = Field(..., alias='file-size-in-bytes')
    file_footer_size_in_bytes: int = Field(..., alias='file-footer-size-in-bytes')
    blob_metadata: list[BlobMetadata] = Field(..., alias='blob-metadata')


class ValueMap(BaseModel):
    keys: list[IntegerTypeValue] | None = Field(
        None, description='List of integer column ids for each corresponding value'
    )
    values: list[PrimitiveTypeValue] | None = Field(
        None, description="List of primitive type values, matched to 'keys' by index"
    )


class DataFile(ContentFile):
    content: Literal['data']
    first_row_id: int | None = Field(
        None,
        alias='first-row-id',
        description='The first row ID assigned to the first row in the data file',
    )
    column_sizes: CountMap | None = Field(
        None,
        alias='column-sizes',
        description='Map of column id to total count, including null and NaN',
    )
    value_counts: CountMap | None = Field(
        None, alias='value-counts', description='Map of column id to null value count'
    )
    null_value_counts: CountMap | None = Field(
        None,
        alias='null-value-counts',
        description='Map of column id to null value count',
    )
    nan_value_counts: CountMap | None = Field(
        None,
        alias='nan-value-counts',
        description='Map of column id to number of NaN values in the column',
    )
    lower_bounds: ValueMap | None = Field(
        None,
        alias='lower-bounds',
        description='Map of column id to lower bound primitive type values',
    )
    upper_bounds: ValueMap | None = Field(
        None,
        alias='upper-bounds',
        description='Map of column id to upper bound primitive type values',
    )


class DeleteFile(RootModel[PositionDeleteFile | EqualityDeleteFile]):
    root: PositionDeleteFile | EqualityDeleteFile = Field(..., discriminator='content')


class FetchScanTasksRequest(BaseModel):
    plan_task: PlanTask = Field(..., alias='plan-task')


class Term(RootModel[Reference | TransformTerm]):
    root: Reference | TransformTerm


class SetStatisticsUpdate(BaseUpdate):
    action: Literal['set-statistics'] = 'set-statistics'
    snapshot_id: int | None = Field(
        None,
        alias='snapshot-id',
        deprecated=True,
        description='This optional field is **DEPRECATED for REMOVAL** since it contains redundant information. Clients should use the `statistics.snapshot-id` field instead.',
    )
    statistics: StatisticsFile


class FunctionDefinitionVersion(BaseModel):
    version_id: int = Field(
        ...,
        alias='version-id',
        description='Monotonically increasing identifier of the definition version.',
    )
    representations: list[FunctionRepresentation] = Field(
        ..., description='UDF implementations.'
    )
    deterministic: bool | None = Field(
        False, description='Whether the function is deterministic.'
    )
    on_null_input: Literal['return-null', 'call'] | None = Field(
        'call',
        alias='on-null-input',
        description='Defines how the UDF behaves when any input parameter is NULL.',
    )
    timestamp_ms: int = Field(
        ...,
        alias='timestamp-ms',
        description='Creation timestamp of this version (unix epoch millis).',
    )


class UnaryExpression(BaseModel):
    type: Literal['is-null', 'not-null', 'is-nan', 'not-nan'] = Field(
        ...,
        examples=[
            [
                'true',
                'false',
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
            ]
        ],
    )
    term: Term


class LiteralExpression(BaseModel):
    type: Literal[
        'lt', 'lt-eq', 'gt', 'gt-eq', 'eq', 'not-eq', 'starts-with', 'not-starts-with'
    ] = Field(
        ...,
        examples=[
            [
                'true',
                'false',
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
            ]
        ],
    )
    term: Term
    value: PrimitiveTypeValue


class SetExpression(BaseModel):
    type: Literal['in', 'not-in'] = Field(
        ...,
        examples=[
            [
                'true',
                'false',
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
            ]
        ],
    )
    term: Term
    values: list[PrimitiveTypeValue]


class StructField(BaseModel):
    id: int
    name: str
    type: Type
    required: bool
    doc: str | None = None
    initial_default: PrimitiveTypeValue | None = Field(None, alias='initial-default')
    write_default: PrimitiveTypeValue | None = Field(None, alias='write-default')


class StructType(BaseModel):
    type: Literal['struct']
    fields: list[StructField]


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


class AndOrExpression(BaseModel):
    type: Literal['and', 'or'] = Field(
        ...,
        examples=[
            [
                'true',
                'false',
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
            ]
        ],
    )
    left: Expression
    right: Expression


class NotExpression(BaseModel):
    type: Literal['not'] = Field(
        ...,
        examples=[
            [
                'true',
                'false',
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
            ]
        ],
    )
    child: Expression


class TableMetadata(BaseModel):
    format_version: int = Field(..., alias='format-version', ge=1, le=3)
    table_uuid: str = Field(..., alias='table-uuid')
    location: str | None = None
    last_updated_ms: int | None = Field(None, alias='last-updated-ms')
    next_row_id: int | None = Field(
        None,
        alias='next-row-id',
        description="A long higher than all assigned row IDs; the next snapshot's first-row-id.",
    )
    properties: dict[str, str] | None = None
    schemas: list[Schema] | None = None
    current_schema_id: int | None = Field(None, alias='current-schema-id')
    last_column_id: int | None = Field(None, alias='last-column-id')
    partition_specs: list[PartitionSpec] | None = Field(None, alias='partition-specs')
    default_spec_id: int | None = Field(None, alias='default-spec-id')
    last_partition_id: int | None = Field(None, alias='last-partition-id')
    sort_orders: list[SortOrder] | None = Field(None, alias='sort-orders')
    default_sort_order_id: int | None = Field(None, alias='default-sort-order-id')
    encryption_keys: list[EncryptedKey] | None = Field(None, alias='encryption-keys')
    snapshots: list[Snapshot] | None = None
    refs: SnapshotReferences | None = None
    current_snapshot_id: int | None = Field(None, alias='current-snapshot-id')
    last_sequence_number: int | None = Field(None, alias='last-sequence-number')
    snapshot_log: SnapshotLog | None = Field(None, alias='snapshot-log')
    metadata_log: MetadataLog | None = Field(None, alias='metadata-log')
    statistics: list[StatisticsFile] | None = None
    partition_statistics: list[PartitionStatisticsFile] | None = Field(
        None, alias='partition-statistics'
    )


class ViewMetadata(BaseModel):
    view_uuid: str = Field(..., alias='view-uuid')
    format_version: int = Field(..., alias='format-version', ge=1, le=1)
    location: str
    current_version_id: int = Field(..., alias='current-version-id')
    versions: list[ViewVersion]
    version_log: list[ViewHistoryEntry] = Field(..., alias='version-log')
    schemas: list[Schema]
    properties: dict[str, str] | None = None


class AddSchemaUpdate(BaseUpdate):
    action: Literal['add-schema'] = 'add-schema'
    schema_: Schema = Field(..., alias='schema')
    last_column_id: int | None = Field(
        None,
        alias='last-column-id',
        deprecated=True,
        description="This optional field is **DEPRECATED for REMOVAL** since it more safe to handle this internally, and shouldn't be exposed to the clients.\nThe highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side.",
    )


class ReadRestrictions(BaseModel):
    """
    Read restrictions for a table, including column projections and row filter expressions.
    A client MUST enforce the restrictions defined in this object when reading data from the table.
    These restrictions apply only to the authenticated principal, user, or account associated with the request. They MUST NOT be interpreted as global policy and MUST NOT be applied beyond the entity identified by the Authentication header (or other applicable authentication mechanism).
    If both properties are absent or empty, the ReadRestrictions object imposes no restrictions and is equivalent to the field being absent from the response. A server MUST NOT return an action for a column whose type is not listed in that action's "Applicable to" set. For all actions, if the input column value is NULL, the output MUST be NULL.

    """

    required_column_projections: (
        list[
            MaskAlphanum
            | MaskToDefault
            | ReplaceWithNull
            | ShowFirst4
            | ShowLast4
            | TruncateToYear
            | TruncateToMonth
            | Sha256Global
            | Sha256QueryLocal
            | ApplyExpression
        ]
        | None
    ) = Field(
        None,
        alias='required-column-projections',
        description="A list of columns that require specific actions to be applied when reading.\nIf this property is absent, a reader MAY access all columns of the table as-is without any mandatory transformations.\nIf this property is present, each listed column MUST have its specified action applied. Columns not listed in required-column-projections are not subject to any read restrictions.\nWhen this list is present:\n1. For each column listed in required-column-projections, the reader MUST apply\n  the specified action before returning values for that column.\n\n2. The reader MUST replace all output references to the column with the result\n  of the action, presenting the result under the original column name. For\n  example, if the action for column cc is mask-alphanum, the reader MUST\n  return the masked value as cc in the query output.\n\n3. Columns not listed in required-column-projections MAY be projected normally\n  by the reader without any mandatory transformations.\n\n4. A column MUST appear at most once in required-column-projections.\n5. If a projected column's action cannot be evaluated by the reader\n  (including unrecognized action types), the reader MUST fail rather than\n  ignore or skip the action.\n\n6. Each action defines the output type for its column. For all predefined\n  actions except apply-expression, the output type matches the input column\n  type. For apply-expression, the output type is determined by the expression.\n",
    )
    required_row_filter: Expression | None = Field(
        None,
        alias='required-row-filter',
        description='An expression that filters rows in the table that the authenticated principal does not have access to.\n1. A reader MUST discard any row for which the filter evaluates to false or null, and\n  no information derived from discarded rows MAY be included in the query result.\n\n2. Row filters MUST be evaluated against the original, untransformed column values.\n  Required projections MUST be applied only after row filters are applied.\n\n3. If a client cannot interpret or evaluate a provided filter expression, it MUST fail.\n4. If this property is absent, null, or always true then no mandatory filtering is required.\n',
    )


class ApplyExpression(Action):
    """
    Replace the field with the result of an expression. Produce the original field name with the expression result.
    Applicable to: all data types

    """

    action: Literal['apply-expression']
    expression: Expression


class LoadTableResult(BaseModel):
    """
    Result used when a table is successfully loaded.


    The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed.
    Clients can check whether metadata has changed by comparing metadata locations after the table has been created.


    The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.


    The following configurations should be respected by clients:

    ## General Configurations

    - `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled
    - `scan-planning-mode`: Communicates to clients the supported planning mode. Clients should use this value to fail fast if the supported scanning mode is not available on the client. Valid values:
      - `client`: Clients MUST use client-side scan planning
      - `server`: Clients MUST use server-side scan planning via the `planTableScan` endpoint

    ## AWS Configurations

    The following configurations should be respected when working with tables stored in AWS S3
     - `client.region`: region to configure client for making requests to AWS
     - `s3.access-key-id`: id for credentials that provide access to the data in S3
     - `s3.secret-access-key`: secret for credentials that provide access to data in S3
     - `s3.session-token`: if present, this value should be used for as the session token
     - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the `RemoteSignRequest` schema section of this spec document.
     - `s3.cross-region-access-enabled`: if `true`, S3 Cross-Region bucket access is enabled

    ## Storage Credentials

    Credentials for ADLS / GCS / S3 / ... are provided through the `storage-credentials` field.
    Clients must first check whether the respective credentials exist in the `storage-credentials` field before checking the `config` for credentials.

    ## Remote Signing

    If remote signing for a specific storage provider is enabled, clients must respect the following configurations when creating a remote signer client:
     - `signer.endpoint`: the remote signer endpoint. Required. Can either be a relative path (to be resolved against `signer.uri`) or an absolute URI.
     - `signer.uri`: the base URI to resolve `signer.endpoint` against. Optional. Only meaningful if `signer.endpoint` is a relative path. Defaults to the catalog's base URI if not set.

    """

    metadata_location: str | None = Field(
        None,
        alias='metadata-location',
        description='May be null if the table is staged as part of a transaction',
    )
    metadata: TableMetadata
    config: dict[str, str] | None = None
    storage_credentials: list[StorageCredential] | None = Field(
        None, alias='storage-credentials'
    )
    read_restrictions: ReadRestrictions | None = Field(None, alias='read-restrictions')


class ScanTasks(BaseModel):
    """
    Scan and planning tasks for server-side scan planning

    - `plan-tasks` contains opaque units of planning work
    - `file-scan-tasks` contains a partial or complete list of table scan tasks
    - `delete-files` contains delete files referenced by file scan tasks

    Each plan task must be passed to the fetchScanTasks endpoint to fetch the file scan tasks for the plan task.

    The list of delete files must contain all delete files referenced by the file scan tasks.

    """

    delete_files: list[DeleteFile] | None = Field(
        None,
        alias='delete-files',
        description='Delete files referenced by file scan tasks',
    )
    file_scan_tasks: list[FileScanTask] | None = Field(None, alias='file-scan-tasks')
    plan_tasks: list[PlanTask] | None = Field(None, alias='plan-tasks')


class CommitTableRequest(BaseModel):
    identifier: TableIdentifier | None = Field(
        None,
        description='Table identifier to update; must be present for CommitTransactionRequest',
    )
    requirements: list[
        AssertCreate
        | AssertTableUUID
        | AssertRefSnapshotId
        | AssertLastAssignedFieldId
        | AssertCurrentSchemaId
        | AssertLastAssignedPartitionId
        | AssertDefaultSpecId
        | AssertDefaultSortOrderId
    ]
    updates: list[TableUpdate]


class CommitViewRequest(BaseModel):
    identifier: TableIdentifier | None = Field(
        None, description='View identifier to update'
    )
    requirements: list[ViewRequirement] | None = None
    updates: list[ViewUpdate]


class CommitTransactionRequest(BaseModel):
    table_changes: list[CommitTableRequest] = Field(..., alias='table-changes')


class CreateTableRequest(BaseModel):
    name: str
    location: str | None = None
    schema_: Schema = Field(..., alias='schema')
    partition_spec: PartitionSpec | None = Field(None, alias='partition-spec')
    write_order: SortOrder | None = Field(None, alias='write-order')
    stage_create: bool | None = Field(None, alias='stage-create')
    properties: dict[str, str] | None = None


class UnregisterTableResult(BaseModel):
    """
    Last metadata location and the corresponding table metadata for the table that was successfully unregistered and is no longer tracked by the catalog.
    """

    metadata_location: str = Field(
        ...,
        alias='metadata-location',
        description='The last metadata location for the table at the time it was unregistered.',
    )
    metadata: TableMetadata


class CreateViewRequest(BaseModel):
    name: str
    location: str | None = None
    schema_: Schema = Field(..., alias='schema')
    view_version: ViewVersion = Field(
        ...,
        alias='view-version',
        description='The view version to create, will replace the schema-id sent within the view-version with the id assigned to the provided schema',
    )
    properties: dict[str, str]


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
    config: dict[str, str] | None = None


class ScanReport(BaseModel):
    table_name: str = Field(..., alias='table-name')
    snapshot_id: int = Field(..., alias='snapshot-id')
    filter: Expression
    schema_id: int = Field(..., alias='schema-id')
    projected_field_ids: list[int] = Field(..., alias='projected-field-ids')
    projected_field_names: list[str] = Field(..., alias='projected-field-names')
    metrics: Metrics
    metadata: dict[str, str] | None = None


class LoadFunctionResult(BaseModel):
    """
    Result returned when a function is loaded from the catalog.


    The function metadata JSON is returned in the `metadata` field. The location of the metadata
    file is returned in the `metadata-location` field, if available.

    """

    metadata_location: str | None = Field(None, alias='metadata-location')
    metadata: FunctionMetadata


class FunctionMetadata(BaseModel):
    """
    Portable UDF metadata format.


    Each function is represented by a self-contained metadata file. The `format-version` field
    identifies the UDF metadata format.

    """

    function_uuid: UUID = Field(
        ...,
        alias='function-uuid',
        description='A UUID that identifies this UDF, generated once at creation.',
    )
    format_version: int = Field(
        ...,
        alias='format-version',
        description='UDF specification format version (must be 1).',
        ge=1,
        le=1,
    )
    definitions: list[FunctionDefinition] = Field(
        ..., description='List of function definition entities.'
    )
    definition_log: list[FunctionDefinitionLogEntry] = Field(
        ...,
        alias='definition-log',
        description="History of versions within the function's definitions.",
    )
    location: str | None = Field(
        None,
        description="The function's base location. This is used to store function metadata files.",
    )
    properties: dict[str, str] | None = Field(
        None, description='A string-to-string map of properties.'
    )
    secure: bool | None = Field(False, description='Whether it is a secure function.')
    doc: str | None = Field(None, description='Documentation string.')


class FunctionDefinition(BaseModel):
    definition_id: str = Field(
        ...,
        alias='definition-id',
        description='A canonical string derived from the parameter types, formatted as a comma-separated list with no spaces.',
    )
    parameters: list[FunctionParameter] = Field(
        ...,
        description='Ordered list of function parameters. Invocation order must match this list.',
    )
    return_type: FunctionDataType = Field(..., alias='return-type')
    return_nullable: bool | None = Field(
        True,
        alias='return-nullable',
        description='A hint to indicate whether the return value is nullable or not.',
    )
    versions: list[FunctionDefinitionVersion] = Field(
        ..., description='Versioned implementations of this definition.'
    )
    current_version_id: int = Field(
        ...,
        alias='current-version-id',
        description='Identifier of the current version for this definition.',
    )
    function_type: Literal['udf', 'udtf'] = Field(
        ...,
        alias='function-type',
        description='Function type. When set to "udtf", "return-type" must be a struct describing the output schema.',
    )
    doc: str | None = Field(None, description='Documentation string.')


class FunctionParameter(BaseModel):
    type: FunctionDataType
    name: str
    doc: str | None = Field(None, description='Parameter documentation.')


class FunctionListType(BaseModel):
    """
    UDF list type object.
    """

    type: Literal['list']
    element: FunctionDataType


class FunctionMapType(BaseModel):
    """
    UDF map type object.
    """

    type: Literal['map']
    key: FunctionDataType
    value: FunctionDataType


class FunctionStructType(BaseModel):
    """
    UDF struct type object.
    """

    type: Literal['struct']
    fields: list[FunctionStructField]


class FunctionStructField(BaseModel):
    """
    UDF struct field.
    """

    name: str
    type: FunctionDataType


class CommitTableResponse(BaseModel):
    metadata_location: str = Field(..., alias='metadata-location')
    metadata: TableMetadata


class PlanTableScanRequest(BaseModel):
    snapshot_id: int | None = Field(
        None,
        alias='snapshot-id',
        description='Identifier for the snapshot to scan in a point-in-time scan',
    )
    select: list[FieldName] | None = Field(
        None, description='List of selected schema fields'
    )
    filter: Expression | None = Field(
        None, description='Expression used to filter the table data'
    )
    min_rows_requested: int | None = Field(
        None,
        alias='min-rows-requested',
        description='The minimum number of rows requested for the scan. This is used as a hint to the server to not have to return more rows than necessary. It is not required for the server to return that many rows since the scan may not produce that many rows. The server can also return more rows than requested.',
    )
    case_sensitive: bool | None = Field(
        True,
        alias='case-sensitive',
        description='Enables case sensitive field matching for filter and select',
    )
    use_snapshot_schema: bool | None = Field(
        False,
        alias='use-snapshot-schema',
        description='Whether to use the schema at the time the snapshot was written.\nWhen time travelling, the snapshot schema should be used (true). When scanning a branch, the table schema should be used (false).',
    )
    start_snapshot_id: int | None = Field(
        None,
        alias='start-snapshot-id',
        description='Starting snapshot ID for an incremental scan (exclusive)',
    )
    end_snapshot_id: int | None = Field(
        None,
        alias='end-snapshot-id',
        description='Ending snapshot ID for an incremental scan (inclusive).\nRequired when start-snapshot-id is specified.',
    )
    stats_fields: list[FieldName] | None = Field(
        None,
        alias='stats-fields',
        description='List of fields for which the service should send column stats.',
    )


class FileScanTask(BaseModel):
    data_file: DataFile = Field(..., alias='data-file')
    delete_file_references: list[int] | None = Field(
        None,
        alias='delete-file-references',
        description='A list of indices in the delete files array (0-based)',
    )
    residual_filter: Expression | None = Field(
        None,
        alias='residual-filter',
        description='An optional filter to be applied to rows in this file scan task.\nIf the residual is not present, the client must produce the residual or use the original filter.',
    )


class Schema(StructType):
    schema_id: int | None = Field(None, alias='schema-id')
    identifier_field_ids: list[int] | None = Field(None, alias='identifier-field-ids')


class Type(RootModel[PrimitiveType | StructType | ListType | MapType]):
    root: PrimitiveType | StructType | ListType | MapType


class Expression(
    RootModel[
        TrueExpression
        | FalseExpression
        | AndOrExpression
        | NotExpression
        | SetExpression
        | LiteralExpression
        | UnaryExpression
    ]
):
    root: (
        TrueExpression
        | FalseExpression
        | AndOrExpression
        | NotExpression
        | SetExpression
        | LiteralExpression
        | UnaryExpression
    )


class TableUpdate(
    RootModel[
        AssignUUIDUpdate
        | UpgradeFormatVersionUpdate
        | AddSchemaUpdate
        | SetCurrentSchemaUpdate
        | AddPartitionSpecUpdate
        | SetDefaultSpecUpdate
        | AddSortOrderUpdate
        | SetDefaultSortOrderUpdate
        | AddSnapshotUpdate
        | SetSnapshotRefUpdate
        | RemoveSnapshotsUpdate
        | RemoveSnapshotRefUpdate
        | SetLocationUpdate
        | SetPropertiesUpdate
        | RemovePropertiesUpdate
        | SetStatisticsUpdate
        | RemoveStatisticsUpdate
        | SetPartitionStatisticsUpdate
        | RemovePartitionStatisticsUpdate
        | RemovePartitionSpecsUpdate
        | RemoveSchemasUpdate
        | AddEncryptionKeyUpdate
        | RemoveEncryptionKeyUpdate
    ]
):
    root: (
        AssignUUIDUpdate
        | UpgradeFormatVersionUpdate
        | AddSchemaUpdate
        | SetCurrentSchemaUpdate
        | AddPartitionSpecUpdate
        | SetDefaultSpecUpdate
        | AddSortOrderUpdate
        | SetDefaultSortOrderUpdate
        | AddSnapshotUpdate
        | SetSnapshotRefUpdate
        | RemoveSnapshotsUpdate
        | RemoveSnapshotRefUpdate
        | SetLocationUpdate
        | SetPropertiesUpdate
        | RemovePropertiesUpdate
        | SetStatisticsUpdate
        | RemoveStatisticsUpdate
        | SetPartitionStatisticsUpdate
        | RemovePartitionStatisticsUpdate
        | RemovePartitionSpecsUpdate
        | RemoveSchemasUpdate
        | AddEncryptionKeyUpdate
        | RemoveEncryptionKeyUpdate
    )


class ViewUpdate(
    RootModel[
        AssignUUIDUpdate
        | UpgradeFormatVersionUpdate
        | AddSchemaUpdate
        | SetLocationUpdate
        | SetPropertiesUpdate
        | RemovePropertiesUpdate
        | AddViewVersionUpdate
        | SetCurrentViewVersionUpdate
    ]
):
    root: (
        AssignUUIDUpdate
        | UpgradeFormatVersionUpdate
        | AddSchemaUpdate
        | SetLocationUpdate
        | SetPropertiesUpdate
        | RemovePropertiesUpdate
        | AddViewVersionUpdate
        | SetCurrentViewVersionUpdate
    )


class CompletedPlanningResult(ScanTasks):
    """
    Completed server-side planning result
    """

    status: Literal['completed'] = Field(
        ..., description='Status of a server-side planning operation'
    )
    storage_credentials: list[StorageCredential] | None = Field(
        None,
        alias='storage-credentials',
        description='Storage credentials for accessing the files returned in the scan result.\nIf the server returns storage credentials as part of the completed scan planning response, the expectation is for the client to use these credentials to read the files returned in the FileScanTasks as part of the scan result.',
    )


class FetchScanTasksResult(ScanTasks):
    """
    Response schema for fetchScanTasks
    """


class ReportMetricsRequest1(ScanReport):
    report_type: str = Field(..., alias='report-type')


class FunctionDataType(
    RootModel[str | FunctionListType | FunctionMapType | FunctionStructType]
):
    root: str | FunctionListType | FunctionMapType | FunctionStructType = Field(
        ...,
        description='A type for function parameters or return value. It is encoded either as a type string or as a JSON object for nested types (struct, list, map) following the UDF spec Types section.\n\nPrimitive and semi-structured type strings are encoded based on the Iceberg type JSON representation (e.g., "int", "string", "timestamp", "decimal(9,2)", "variant"). Type strings must contain no spaces or quote characters.\n\nNested types are based on the Iceberg type JSON representation, but the UDF spec only requires a subset of fields (e.g., list requires `type` and `element`; map requires `type`, `key`, and `value`; struct requires `type` and `fields`). Any other fields must be ignored.\n',
    )


class CompletedPlanningWithIDResult(CompletedPlanningResult):
    plan_id: str = Field(
        ..., alias='plan-id', description='ID used to track a planning request'
    )
    status: Literal['completed']


class FetchPlanningResult(
    RootModel[CompletedPlanningResult | FailedPlanningResult | EmptyPlanningResult]
):
    root: CompletedPlanningResult | FailedPlanningResult | EmptyPlanningResult = Field(
        ...,
        description='Result of server-side scan planning for fetchPlanningResult',
        discriminator='status',
    )


class ReportMetricsRequest(RootModel[ReportMetricsRequest1 | ReportMetricsRequest2]):
    root: ReportMetricsRequest1 | ReportMetricsRequest2


class PlanTableScanResult(
    RootModel[
        CompletedPlanningWithIDResult
        | FailedPlanningResult
        | AsyncPlanningResult
        | EmptyPlanningResult
    ]
):
    root: (
        CompletedPlanningWithIDResult
        | FailedPlanningResult
        | AsyncPlanningResult
        | EmptyPlanningResult
    ) = Field(
        ...,
        description='Result of server-side scan planning for planTableScan',
        discriminator='status',
    )


StructField.model_rebuild()
ListType.model_rebuild()
MapType.model_rebuild()
AndOrExpression.model_rebuild()
NotExpression.model_rebuild()
TableMetadata.model_rebuild()
ViewMetadata.model_rebuild()
AddSchemaUpdate.model_rebuild()
ReadRestrictions.model_rebuild()
ApplyExpression.model_rebuild()
ScanTasks.model_rebuild()
CommitTableRequest.model_rebuild()
CommitViewRequest.model_rebuild()
CreateTableRequest.model_rebuild()
CreateViewRequest.model_rebuild()
ScanReport.model_rebuild()
LoadFunctionResult.model_rebuild()
FunctionMetadata.model_rebuild()
FunctionDefinition.model_rebuild()
FunctionParameter.model_rebuild()
FunctionListType.model_rebuild()
FunctionMapType.model_rebuild()
FunctionStructType.model_rebuild()
FunctionStructField.model_rebuild()
PlanTableScanRequest.model_rebuild()
FileScanTask.model_rebuild()
CompletedPlanningResult.model_rebuild()
FetchScanTasksResult.model_rebuild()
ReportMetricsRequest1.model_rebuild()
CompletedPlanningWithIDResult.model_rebuild()
