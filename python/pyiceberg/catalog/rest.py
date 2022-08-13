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
from json import JSONDecodeError
from typing import (
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Type,
    Union,
)

import requests
from pydantic import Field
from requests import HTTPError

from pyiceberg import __version__
from pyiceberg.catalog import (
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    AuthorizationExpiredError,
    BadCredentialsError,
    BadRequestError,
    ForbiddenError,
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    OAuthError,
    RESTError,
    ServerError,
    ServiceUnavailableError,
    TableAlreadyExistsError,
    UnauthorizedError,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataV1, TableMetadataV2
from pyiceberg.table.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel


class Endpoints:
    get_config: str = "config"
    list_namespaces: str = "namespaces"
    create_namespace: str = "namespaces"
    load_namespace_metadata: str = "namespaces/{namespace}"
    drop_namespace: str = "namespaces/{namespace}"
    update_properties: str = "namespaces/{namespace}/properties"
    list_tables: str = "namespaces/{namespace}/tables"
    create_table: str = "namespaces/{namespace}/tables"
    load_table: str = "namespaces/{namespace}/tables/{table}"
    update_table: str = "namespaces/{namespace}/tables/{table}"
    drop_table: str = "namespaces/{namespace}/tables/{table}?purgeRequested={purge}"
    table_exists: str = "namespaces/{namespace}/tables/{table}"
    get_token: str = "oauth/tokens"
    rename_table: str = "tables/rename"


AUTHORIZATION_HEADER = "Authorization"
BEARER_PREFIX = "Bearer"
CATALOG_SCOPE = "catalog"
CLIENT_ID = "client_id"
PREFIX = "prefix"
CLIENT_SECRET = "client_secret"
CLIENT_CREDENTIALS = "client_credentials"
CREDENTIAL = "credential"
GRANT_TYPE = "grant_type"
SCOPE = "scope"
TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"

NAMESPACE_SEPARATOR = b"\x1F".decode("UTF-8")


class TableResponse(IcebergBaseModel):
    metadata_location: Optional[str] = Field(alias="metadata-location", default=None)
    metadata: Union[TableMetadataV1, TableMetadataV2] = Field()
    config: Properties = Field(default_factory=dict)


class CreateTableRequest(IcebergBaseModel):
    name: str = Field()
    location: Optional[str] = Field()
    table_schema: Schema = Field(alias="schema")
    partition_spec: Optional[PartitionSpec] = Field(alias="partition-spec")
    write_order: Optional[SortOrder] = Field(alias="write-order")
    stage_create: bool = Field(alias="stage-create", default=False)
    properties: Properties = Field(default_factory=dict)


class TokenResponse(IcebergBaseModel):
    access_token: str = Field()
    token_type: str = Field()
    expires_in: int = Field()
    issued_token_type: str = Field()


class ConfigResponse(IcebergBaseModel):
    defaults: Properties = Field()
    overrides: Properties = Field()


class ListNamespaceResponse(IcebergBaseModel):
    namespaces: List[Identifier] = Field()


class NamespaceResponse(IcebergBaseModel):
    namespace: Identifier = Field()
    properties: Properties = Field()


class UpdateNamespacePropertiesResponse(IcebergBaseModel):
    removed: List[str] = Field()
    updated: List[str] = Field()
    missing: List[str] = Field()


class ListTableResponseEntry(IcebergBaseModel):
    name: str = Field()
    namespace: Identifier = Field()


class ListTablesResponse(IcebergBaseModel):
    identifiers: List[ListTableResponseEntry] = Field()


class ErrorResponseMessage(IcebergBaseModel):
    message: str = Field()
    type: str = Field()
    code: int = Field()


class ErrorResponse(IcebergBaseModel):
    error: ErrorResponseMessage = Field()


class OAuthErrorResponse(IcebergBaseModel):
    error: Literal[
        "invalid_request", "invalid_client", "invalid_grant", "unauthorized_client", "unsupported_grant_type", "invalid_scope"
    ]
    error_description: Optional[str]
    error_uri: Optional[str]


class RestCatalog(Catalog):
    token: str
    config: Properties

    uri: str

    def __init__(
        self,
        name: str,
        uri: str,
        credential: Optional[str] = None,
        token: Optional[str] = None,
        **properties: str,
    ):
        """Rest Catalog

        You either need to provide a client_id and client_secret, or an already valid token.

        Args:
            name: Name to identify the catalog
            uri: The base-url of the REST Catalog endpoint
            credential: The credentials for authentication against the client
            token: The bearer token
            properties: Properties that are passed along to the configuration
        """
        self.uri = uri
        if credential:
            self.token = self._fetch_access_token(credential)
        elif token:
            self.token = token
        self.config = self._fetch_config(properties)
        super().__init__(name, **properties)

    def _check_valid_namespace_identifier(self, identifier: Union[str, Identifier]) -> Identifier:
        """The identifier should have at least one element"""
        identifier_tuple = Catalog.identifier_to_tuple(identifier)
        if len(identifier_tuple) < 1:
            raise NoSuchNamespaceError(f"Empty namespace identifier: {identifier}")
        return identifier_tuple

    @property
    def headers(self) -> Properties:
        headers = {
            "Content-type": "application/json",
            "X-Client-Version": __version__,
        }
        if self.token:
            headers[AUTHORIZATION_HEADER] = f"{BEARER_PREFIX} {self.token}"
        return headers

    def url(self, endpoint: str, prefixed: bool = True, **kwargs) -> str:
        """Constructs the endpoint

        Args:
            endpoint: Resource identifier that points to the REST catalog
            prefixed: If the prefix return by the config needs to be appended

        Returns:
            The base url of the rest catalog
        """

        url = self.uri
        url = url + "v1/" if url.endswith("/") else url + "/v1/"

        if prefixed:
            url += self.config.get(PREFIX, "")
            url = url if url.endswith("/") else url + "/"

        return url + endpoint.format(**kwargs)

    def _fetch_access_token(self, credential: str) -> str:
        client_id, client_secret = credential.split(":")
        data = {GRANT_TYPE: CLIENT_CREDENTIALS, CLIENT_ID: client_id, CLIENT_SECRET: client_secret, SCOPE: CATALOG_SCOPE}
        url = self.url(Endpoints.get_token, prefixed=False)
        # Uses application/x-www-form-urlencoded by default
        response = requests.post(url=url, data=data)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {400: OAuthError, 401: BadCredentialsError})

        return TokenResponse(**response.json()).access_token

    def _fetch_config(self, properties: Properties) -> Properties:
        response = requests.get(self.url(Endpoints.get_config, prefixed=False), headers=self.headers)
        response.raise_for_status()
        config_response = ConfigResponse(**response.json())
        config = config_response.defaults
        config.update(properties)
        config.update(config_response.overrides)
        return config

    def _split_identifier_for_path(self, identifier: Union[str, Identifier]) -> Properties:
        identifier_tuple = self.identifier_to_tuple(identifier)
        if len(identifier_tuple) <= 1:
            raise NoSuchTableError(f"Missing namespace or invalid identifier: {'.'.join(identifier_tuple)}")
        return {"namespace": NAMESPACE_SEPARATOR.join(identifier_tuple[:-1]), "table": identifier_tuple[-1]}

    def _split_identifier_for_json(self, identifier: Union[str, Identifier]) -> Dict[str, Union[Identifier, str]]:
        identifier_tuple = self.identifier_to_tuple(identifier)
        if len(identifier_tuple) <= 1:
            raise NoSuchTableError(f"Missing namespace or invalid identifier: {identifier_tuple}")
        return {"namespace": identifier_tuple[:-1], "name": identifier_tuple[-1]}

    def _handle_non_200_response(self, exc: HTTPError, error_handler: Dict[int, Type[Exception]]):
        exception: Type[Exception]
        code = exc.response.status_code
        if code in error_handler:
            exception = error_handler[code]
        elif code == 400:
            exception = BadRequestError
        elif code == 401:
            exception = UnauthorizedError
        elif code == 403:
            exception = ForbiddenError
        elif code == 422:
            exception = RESTError
        elif code == 419:
            exception = AuthorizationExpiredError
        elif code == 501:
            exception = NotImplementedError
        elif code == 503:
            exception = ServiceUnavailableError
        elif 500 <= code < 600:
            exception = ServerError
        else:
            exception = RESTError

        try:
            if exception == OAuthError:
                # The OAuthErrorResponse has a different format
                error = OAuthErrorResponse(**exc.response.json())
                response = str(error.error)
                if description := error.error_description:
                    response += f": {description}"
                if uri := error.error_uri:
                    response += f" ({uri})"
            else:
                error = ErrorResponse(**exc.response.json()).error
                response = f"{error.type}: {error.message}"
        except JSONDecodeError:
            # In the case we don't have a proper response
            response = f"RESTError {exc.response.status_code}: Could not decode json payload: {exc.response.text}"

        raise exception(response) from exc

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        namespace_and_table = self._split_identifier_for_path(identifier)
        request = CreateTableRequest(
            name=namespace_and_table["table"],
            location=location,
            table_schema=schema,
            partition_spec=partition_spec,
            write_order=sort_order,
            properties=properties,
        )
        serialized_json = request.json()
        response = requests.post(
            self.url(Endpoints.create_table, namespace=namespace_and_table["namespace"]),
            data=serialized_json,
            headers=self.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {409: TableAlreadyExistsError})

        table_response = TableResponse(**response.json())

        return Table(
            identifier=(self.name,) + self.identifier_to_tuple(identifier),
            metadata_location=table_response.metadata_location,
            metadata=table_response.metadata,
        )

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        namespace_concat = NAMESPACE_SEPARATOR.join(namespace_tuple)
        response = requests.get(
            self.url(Endpoints.list_tables, namespace=namespace_concat),
            headers=self.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError})
        return [(*table.namespace, table.name) for table in ListTablesResponse(**response.json()).identifiers]

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        identifier_tuple = self.identifier_to_tuple(identifier)

        if len(identifier_tuple) <= 1:
            raise NoSuchTableError(f"Missing namespace or invalid identifier: {identifier}")

        response = requests.get(
            self.url(Endpoints.load_table, prefixed=True, **self._split_identifier_for_path(identifier)), headers=self.headers
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError})

        table_response = TableResponse(**response.json())

        return Table(
            identifier=(self.name,) + identifier_tuple,
            metadata_location=table_response.metadata_location,
            metadata=table_response.metadata,
        )

    def drop_table(self, identifier: Union[str, Identifier], purge_requested: bool = False) -> None:
        response = requests.delete(
            self.url(Endpoints.drop_table, prefixed=True, purge=purge_requested, **self._split_identifier_for_path(identifier)),
            headers=self.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError})

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self.drop_table(identifier=identifier, purge_requested=True)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]):
        payload = {
            "source": self._split_identifier_for_json(from_identifier),
            "destination": self._split_identifier_for_json(to_identifier),
        }
        response = requests.post(self.url(Endpoints.rename_table), json=payload, headers=self.headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError, 409: TableAlreadyExistsError})

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        payload = {"namespace": namespace_tuple, "properties": properties}
        response = requests.post(self.url(Endpoints.create_namespace), json=payload, headers=self.headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError, 409: NamespaceAlreadyExistsError})

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        namespace = NAMESPACE_SEPARATOR.join(namespace_tuple)
        response = requests.delete(self.url(Endpoints.drop_namespace, namespace=namespace), headers=self.headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError})

    def list_namespaces(self) -> List[Identifier]:
        response = requests.get(self.url(Endpoints.list_namespaces), headers=self.headers)
        response.raise_for_status()
        namespaces = ListNamespaceResponse(**response.json())
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {})
        return namespaces.namespaces

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        namespace = NAMESPACE_SEPARATOR.join(namespace_tuple)
        response = requests.get(self.url(Endpoints.load_namespace_metadata, namespace=namespace), headers=self.headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError})

        return NamespaceResponse(**response.json()).properties

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        namespace = NAMESPACE_SEPARATOR.join(namespace_tuple)
        payload = {"removals": list(removals or []), "updates": updates}
        print(f"{payload}")
        response = requests.post(self.url(Endpoints.update_properties, namespace=namespace), json=payload, headers=self.headers)
        print(f"{response.json()}")
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError})
        parsed_response = UpdateNamespacePropertiesResponse(**response.json())
        return PropertiesUpdateSummary(
            removed=parsed_response.removed,
            updated=parsed_response.updated,
            missing=parsed_response.missing,
        )
