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
from typing import (
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import requests
from pydantic import Field
from requests import HTTPError

from pyiceberg.catalog import Identifier, Properties
from pyiceberg.catalog.base import Catalog, PropertiesUpdateSummary
from pyiceberg.exceptions import (
    AlreadyExistsError,
    BadCredentialsError,
    BadRequestError,
    NoSuchNamespaceError,
    NoSuchTableError,
    RESTError,
    ServerError,
    ServiceUnavailableError,
    TableAlreadyExistsError,
    UnauthorizedError,
)
from pyiceberg.schema import Schema
from pyiceberg.table.base import Table
from pyiceberg.table.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
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


class CreateTableRequest(IcebergBaseModel):
    name: str = Field()
    location: Optional[str] = Field()
    table_schema: Schema = Field(alias="schema")
    partition_spec: Optional[PartitionSpec] = Field(alias="partition-spec")
    write_order: Optional[SortOrder] = Field(alias="write-order")
    stage_creation: bool = Field(alias="stage-creation", default=False)
    properties: Dict[str, str] = Field(default_factory=dict)


class TokenResponse(IcebergBaseModel):
    access_token: str = Field()
    token_type: str = Field()
    expires_in: int = Field()
    warehouse_id: str = Field()
    region: str = Field()
    issued_token_type: str = Field()


class ConfigResponse(IcebergBaseModel):
    defaults: Dict[str, str] = Field()
    overrides: Dict[str, str] = Field()


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


class RestCatalog(Catalog):
    token: TokenResponse
    config: ConfigResponse

    host: str

    def __init__(self, name: str, properties: Properties, host: str, token: str):
        self.host = host
        self.token = self._fetch_access_token(token)
        self.config = self._fetch_config()
        super().__init__(name, properties)

    def _split_token(self, token: str) -> Tuple[str, str]:
        """Splits the token in a client id and secret

        Args:
            token: The token with a semicolon as a separator

        Returns:
            The client id and secret
        """
        client, secret = token.split(":")
        return client, secret

    @property
    def headers(self) -> Dict[str, str]:
        return {AUTHORIZATION_HEADER: f"{BEARER_PREFIX} {self.token.access_token}", "Content-type": "application/json"}

    def url(self, endpoint: str, prefixed: bool = True, **kwargs) -> str:
        """Constructs the endpoint

        Args:
            prefixed: If the prefix return by the config needs to be appended

        Returns:
            The base url of the rest catalog
        """

        url = self.host
        url = url if url.endswith("/") else url + "/"

        if prefixed:
            url += self.config.overrides.get(PREFIX, "")
            url = url if url.endswith("/") else url + "/"

        return url + endpoint.format(**kwargs)

    def _fetch_access_token(self, token: str) -> TokenResponse:
        client, secret = self._split_token(token)
        data = {GRANT_TYPE: CLIENT_CREDENTIALS, CLIENT_ID: client, CLIENT_SECRET: secret, SCOPE: CATALOG_SCOPE}
        response = requests.post(self.url(Endpoints.get_token, prefixed=False), data=data)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {401: BadCredentialsError})

        return TokenResponse(**response.json())

    def _fetch_config(self) -> ConfigResponse:
        response = requests.get(self.url(Endpoints.get_config, prefixed=False), headers=self.headers)
        response.raise_for_status()
        return ConfigResponse(**response.json())

    def _split_namespace_and_table(self, identifier: Union[str, Identifier]) -> Dict[str, str]:
        identifier = self.identifier_to_tuple(identifier)
        return {"namespace": NAMESPACE_SEPARATOR.join(identifier[:-1]), "table": identifier[-1]}

    def _split_namespace_and_name(self, identifier: Union[str, Identifier]) -> Dict[str, Union[Tuple[str, ...], str]]:
        identifier = self.identifier_to_tuple(identifier)
        return {"namespace": identifier[:-1], "name": identifier[-1]}

    def _handle_non_200_response(self, exc: HTTPError, error_handler: Dict[int, Type[Exception]]):
        try:
            response = ErrorResponse(**exc.response.json())
        except Exception:  # pylint: disable=W0012,W0703
            # In the case we don't have a proper response
            response = ErrorResponse(
                error=ErrorResponseMessage(
                    message=f"Could not decode json payload: {exc.response.text}",
                    type="RESTError",
                    code=exc.response.status_code,
                )
            )

        code = exc.response.status_code
        if code in error_handler:
            raise error_handler[code](response) from exc
        elif code == 400:
            raise BadRequestError(response.error.message) from exc
        elif code == 401:
            raise UnauthorizedError(response.error.message) from exc
        elif code == 503:
            raise ServiceUnavailableError(response.error.message) from exc
        elif 500 <= code < 600:
            raise ServerError(response.error.message) from exc
        else:
            raise RESTError(response.error.message) from exc

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Optional[Properties] = None,
    ) -> Table:
        namespace_and_table = self._split_namespace_and_table(identifier)
        properties = properties or {}
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

        return Table(identifier=identifier, **response.json())

    def list_tables(self, namespace: Optional[Union[str, Identifier]] = None) -> List[Identifier]:
        namespace_concat = NAMESPACE_SEPARATOR.join(self.identifier_to_tuple(namespace or ""))
        response = requests.get(
            self.url(Endpoints.list_tables, namespace=namespace_concat),
            headers=self.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError})
        response = ListTablesResponse(**response.json())

        return [(*entry.namespace, entry.name) for entry in response.identifiers]

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        response = requests.get(
            self.url(Endpoints.load_table, prefixed=True, **self._split_namespace_and_table(identifier)), headers=self.headers
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError})
        return Table(identifier=identifier, **response.json())

    def drop_table(self, identifier: Union[str, Identifier], purge_requested: bool = False) -> None:
        response = requests.delete(
            self.url(Endpoints.drop_table, prefixed=True, purge=purge_requested, **self._split_namespace_and_table(identifier)),
            headers=self.headers,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError})

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError("Not implemented")

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]):
        payload = {
            "source": self._split_namespace_and_name(from_identifier),
            "destination": self._split_namespace_and_name(to_identifier),
        }
        response = requests.post(self.url(Endpoints.rename_table), json=payload, headers=self.headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError, 409: TableAlreadyExistsError})

    def create_namespace(self, namespace: Union[str, Identifier], properties: Optional[Properties] = None) -> None:
        payload = {"namespace": self.identifier_to_tuple(namespace), "properties": properties}
        response = requests.post(self.url(Endpoints.create_namespace), json=payload, headers=self.headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {409: AlreadyExistsError})

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace = NAMESPACE_SEPARATOR.join(self.identifier_to_tuple(namespace))
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
        namespace = NAMESPACE_SEPARATOR.join(self.identifier_to_tuple(namespace))
        response = requests.get(self.url(Endpoints.load_namespace_metadata, namespace=namespace), headers=self.headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError})

        return NamespaceResponse(**response.json()).properties

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Optional[Properties] = None
    ) -> PropertiesUpdateSummary:
        namespace = NAMESPACE_SEPARATOR.join(self.identifier_to_tuple(namespace))
        payload = {"removals": list(removals or []), "updates": updates}
        response = requests.post(self.url(Endpoints.update_properties, namespace=namespace), json=payload, headers=self.headers)
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
