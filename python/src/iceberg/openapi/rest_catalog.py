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
# generated by datamodel-codegen:
#   filename:  rest-catalog-open-api.yaml

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Extra, Field


class ErrorModel(BaseModel):
    """
    JSON error payload returned in a response with further details on the error
    """

    class Config:
        allow_population_by_field_name = True

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

    class Config:
        allow_population_by_field_name = True

    overrides: Dict[str, Any] = Field(
        ...,
        description='Properties that should be used to override client configuration; applied after defaults and client configuration.',
    )
    defaults: Dict[str, Any] = Field(
        ...,
        description='Properties that should be used as default configuration; applied before client configuration.',
    )


class Updates(BaseModel):
    pass

    class Config:
        allow_population_by_field_name = True


class UpdateNamespacePropertiesRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True

    removals: Optional[List[str]] = Field(
        None, example=['department', 'access_group'], unique_items=True
    )
    updates: Optional[Union[List[str], Updates]] = Field(
        None, example={'owner': 'Hank Bendickson'}, unique_items=True
    )


class Namespace(BaseModel):
    """
    Reference to one or more levels of a namespace
    """

    class Config:
        allow_population_by_field_name = True

    __root__: List[str] = Field(
        ...,
        description='Reference to one or more levels of a namespace',
        example=['accounting', 'tax'],
    )


class TableIdentifier(BaseModel):
    class Config:
        allow_population_by_field_name = True

    namespace: Namespace
    name: str


class PrimitiveType(BaseModel):
    class Config:
        allow_population_by_field_name = True

    __root__: str = Field(..., example=['long', 'string', 'fixed[16]', 'decimal(10,2)'])


class Transform(BaseModel):
    class Config:
        allow_population_by_field_name = True

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
    class Config:
        allow_population_by_field_name = True

    field_id: Optional[int] = Field(None, alias='field-id')
    source_id: int = Field(..., alias='source-id')
    name: str
    transform: Transform


class PartitionSpec(BaseModel):
    class Config:
        allow_population_by_field_name = True

    spec_id: Optional[int] = Field(None, alias='spec-id')
    fields: List[PartitionField]


class SortDirection(Enum):
    asc = 'asc'
    desc = 'desc'


class NullOrder(Enum):
    nulls_first = 'nulls-first'
    nulls_last = 'nulls-last'


class SortField(BaseModel):
    class Config:
        allow_population_by_field_name = True

    source_id: int = Field(..., alias='source-id')
    transform: Transform
    direction: SortDirection
    null_order: NullOrder = Field(..., alias='null-order')


class SortOrder(BaseModel):
    class Config:
        allow_population_by_field_name = True

    order_id: Optional[int] = Field(None, alias='order-id')
    fields: List[SortField]


class Summary(BaseModel):
    class Config:
        allow_population_by_field_name = True

    operation: Literal['append', 'replace', 'overwrite', 'delete']
    additionalProperties: Optional[str] = None


class Snapshot(BaseModel):
    class Config:
        allow_population_by_field_name = True

    snapshot_id: int = Field(..., alias='snapshot-id')
    timestamp_ms: int = Field(..., alias='timestamp-ms')
    manifest_list: str = Field(
        ...,
        alias='manifest-list',
        description="Location of the snapshot's manifest list file",
    )
    schema_id: Optional[int] = Field(None, alias='schema-id')
    summary: Summary


class SnapshotReference(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: Literal['tag', 'branch']
    snapshot_id: int = Field(..., alias='snapshot-id')
    max_ref_age_ms: Optional[int] = Field(None, alias='max-ref-age-ms')
    max_snapshot_age_ms: Optional[int] = Field(None, alias='max-snapshot-age-ms')
    min_snapshots_to_keep: Optional[int] = Field(None, alias='min-snapshots-to-keep')


class SnapshotReferences(BaseModel):
    pass

    class Config:
        extra = Extra.allow
        allow_population_by_field_name = True


class SnapshotLogItem(BaseModel):
    class Config:
        allow_population_by_field_name = True

    snapshot_id: int = Field(..., alias='snapshot-id')
    timestamp_ms: int = Field(..., alias='timestamp-ms')


class SnapshotLog(BaseModel):
    class Config:
        allow_population_by_field_name = True

    __root__: List[SnapshotLogItem]


class MetadataLogItem(BaseModel):
    class Config:
        allow_population_by_field_name = True

    metadata_file: str = Field(..., alias='metadata-file')
    timestamp_ms: int = Field(..., alias='timestamp-ms')


class MetadataLog(BaseModel):
    class Config:
        allow_population_by_field_name = True

    __root__: List[MetadataLogItem]


class BaseUpdate(BaseModel):
    class Config:
        allow_population_by_field_name = True

    action: Literal[
        'upgrade-format-version',
        'add-schema',
        'set-current-schema',
        'add-spec',
        'set-default-spec',
        'add-sort-order',
        'set-default-sort-order',
        'add-snapshot',
        'set-snapshot-ref',
        'remove-snapshots',
        'remove-snapshot-ref',
        'set-location',
        'set-properties',
        'remove-properties',
    ]


class UpgradeFormatVersionUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    format_version: int = Field(..., alias='format-version')


class SetCurrentSchemaUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    schema_id: int = Field(
        ...,
        alias='schema-id',
        description='Schema ID to set as current, or -1 to set last added schema',
    )


class AddPartitionSpecUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    spec: PartitionSpec


class SetDefaultSpecUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    spec_id: int = Field(
        ...,
        alias='spec-id',
        description='Partition spec ID to set as the default, or -1 to set last added spec',
    )


class AddSortOrderUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    sort_order: SortOrder = Field(..., alias='sort-order')


class SetDefaultSortOrderUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    sort_order_id: int = Field(
        ...,
        alias='sort-order-id',
        description='Sort order ID to set as the default, or -1 to set last added sort order',
    )


class AddSnapshotUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    snapshot: Snapshot


class SetSnapshotRefUpdate(BaseUpdate, SnapshotReference):
    class Config:
        allow_population_by_field_name = True

    ref_name: str = Field(..., alias='ref-name')


class RemoveSnapshotsUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    snapshot_ids: List[int] = Field(..., alias='snapshot-ids')


class RemoveSnapshotRefUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    ref_name: str = Field(..., alias='ref-name')


class SetLocationUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    location: str


class SetPropertiesUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    updates: Dict[str, str]


class RemovePropertiesUpdate(BaseUpdate):
    class Config:
        allow_population_by_field_name = True

    removals: List[str]


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

    class Config:
        allow_population_by_field_name = True

    requirement: Literal[
        'assert-create',
        'assert-table-uuid',
        'assert-ref-snapshot-id',
        'assert-last-assigned-field-id',
        'assert-current-schema-id',
        'assert-last-assigned-partition-id',
        'assert-default-spec-id',
        'assert-default-sort-order-id',
    ]
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


class OAuthClientCredentialsRequest(BaseModel):
    """
        OAuth2 client credentials request

    See https://datatracker.ietf.org/doc/html/rfc6749#section-4.4
    """

    class Config:
        allow_population_by_field_name = True

    grant_type: Literal['client_credentials']
    scope: Optional[str] = None
    client_id: str = Field(
        ...,
        description='Client ID\n\nThis can be sent in the request body, but OAuth2 recomments sending it in a Basic Authorization header.',
    )
    client_secret: str = Field(
        ...,
        description='Client secret\n\nThis can be sent in the request body, but OAuth2 recomments sending it in a Basic Authorization header.',
    )


class OAuthTokenExchangeRequest(BaseModel):
    """
        OAuth2 token exchange request

    See https://datatracker.ietf.org/doc/html/rfc8693
    """

    class Config:
        allow_population_by_field_name = True

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
    class Config:
        allow_population_by_field_name = True

    __root__: Union[OAuthClientCredentialsRequest, OAuthTokenExchangeRequest]


class CreateNamespaceRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True

    namespace: Namespace
    properties: Optional[Dict[str, Any]] = Field(
        {},
        description='Configured string to string map of properties for the namespace',
        example={'owner': 'Hank Bendickson'},
    )


class RenameTableRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True

    source: Optional[TableIdentifier] = None
    destination: Optional[TableIdentifier] = None


class StructField(BaseModel):
    class Config:
        allow_population_by_field_name = True

    id: int
    name: str
    required: bool
    type: Type
    doc: Optional[str] = None


class StructType(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: Optional[Literal['struct']] = None
    fields: List[StructField]


class ListType(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: Literal['list']
    element_id: int = Field(..., alias='element-id')
    element_required: bool = Field(..., alias='element-required')
    element: Type


class MapType(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: Literal['map']
    key_id: int = Field(..., alias='key-id')
    key: Type
    value_id: int = Field(..., alias='value-id')
    value_required: bool = Field(..., alias='value-required')
    value: Type


class Type(BaseModel):
    class Config:
        allow_population_by_field_name = True

    __root__: Union[PrimitiveType, StructType, ListType, MapType]


class TableMetadata(BaseModel):
    class Config:
        allow_population_by_field_name = True

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
    class Config:
        allow_population_by_field_name = True

    schema_: Schema = Field(..., alias='schema')


class TableUpdate(BaseModel):
    class Config:
        allow_population_by_field_name = True

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

    The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata must be returned in the `metadata-location` field. Clients can check whether metadata has changed by comparing metadata locations.

    The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.
    """

    class Config:
        allow_population_by_field_name = True

    metadata_location: str = Field(..., alias='metadata-location')
    metadata: TableMetadata
    config: Optional[Dict[str, str]] = None


class CommitTableRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True

    requirements: List[TableRequirement]
    updates: List[TableUpdate]


class CreateTableRequest(BaseModel):
    class Config:
        allow_population_by_field_name = True

    name: str
    location: Optional[str] = None
    schema_: Schema = Field(..., alias='schema')
    partition_spec: Optional[PartitionSpec] = Field(None, alias='partition-spec')
    write_order: Optional[SortOrder] = Field(None, alias='write-order')
    stage_create: Optional[bool] = Field(None, alias='stage-create')
    properties: Optional[Dict[str, str]] = None


class Schema(StructType):
    class Config:
        allow_population_by_field_name = True

    schema_id: Optional[int] = Field(None, alias='schema-id')
    identifier_field_ids: Optional[List[int]] = Field(
        None, alias='identifier-field-ids'
    )


StructField.update_forward_refs()
ListType.update_forward_refs()
MapType.update_forward_refs()
TableMetadata.update_forward_refs()
AddSchemaUpdate.update_forward_refs()
CreateTableRequest.update_forward_refs()
