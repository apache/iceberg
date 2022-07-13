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
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import requests
from pydantic import Field
from requests import HTTPError

from pyiceberg.catalog import Identifier, Properties
from pyiceberg.catalog.base import Catalog
from pyiceberg.exceptions import (
    AlreadyExistsError,
    BadCredentialsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.schema import Schema
from pyiceberg.table.base import PartitionSpec, Table
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
    drop_table: str = "namespaces/{namespace}/tables/{table}"
    table_exists: str = "namespaces/{namespace}/tables/{table}"
    get_token: str = "oauth/tokens"
    rename_table: str = "tables/rename"


AUTHORIZATION_HEADER = "Authorization"
BEARER_PREFIX = "Bearer"
TOKEN = "t-rHkfxlPOv70:Shr3gJy9mpT0SexvRfuqv40SdOg"
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
    removed: List[str]
    updated: List[str]
    missing: List[str]


class ListTableResponseEntry(IcebergBaseModel):
    name: str
    namespace: Identifier


class ListTablesResponse(IcebergBaseModel):
    identifiers: List[ListTableResponseEntry]


class RestCatalog(Catalog):
    token: TokenResponse
    config: ConfigResponse

    host: str

    def __init__(self, name: str, properties: Properties, host: str):
        self.host = host
        self.token = self._fetch_access_token()
        self.config = self._fetch_config()
        super().__init__(name, properties)

    def _get_host(self, prefixed: bool = True) -> str:
        """Resolves the endpoint based

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

        return url

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
    def _headers(self) -> Dict[str, str]:
        return {AUTHORIZATION_HEADER: f"{BEARER_PREFIX} {self.token.access_token}"}

    def _fetch_access_token(self) -> TokenResponse:
        client, secret = self._split_token(TOKEN)
        data = {GRANT_TYPE: CLIENT_CREDENTIALS, CLIENT_ID: client, CLIENT_SECRET: secret, SCOPE: CATALOG_SCOPE}
        response = requests.post(f"{self._get_host(prefixed=False)}{Endpoints.get_token}", data=data)
        try:
            response.raise_for_status()

        except HTTPError as exc:
            code = exc.response.status_code
            error = exc.response.json()["error"]
            if code == 401:
                raise BadCredentialsError(error["message"])

        return TokenResponse(**response.json())

    def _fetch_config(self) -> ConfigResponse:
        response = requests.get(f"{self._get_host(prefixed=False)}{Endpoints.get_config}", headers=self._headers)
        response.raise_for_status()
        return ConfigResponse(**response.json())

    def _split_namespace_and_table(self, identifier: Union[str, Identifier]) -> Dict[str, str]:
        identifier = self.identifier_to_tuple(identifier)
        return {"namespace": NAMESPACE_SEPARATOR.join(identifier[:-1]), "table": identifier[-1]}

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        sort_order: Optional[Any] = None,
        properties: Optional[Properties] = None,
    ) -> Table:
        url = f"{self._get_host()}{Endpoints.create_namespace}"
        headers = {
            AUTHORIZATION_HEADER: f"{BEARER_PREFIX} {self.token.access_token}",
        }
        payload = {
            "name": self.identifier_to_tuple(identifier),
            "location": location,
            "schema": schema,
            "partition-spec": partition_spec,
            "write-order": sort_order,
        }
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return NamespaceResponse(**response.json())

    def list_tables(self, namespace: Optional[Union[str, Identifier]] = None) -> List[Identifier]:
        namespace = NAMESPACE_SEPARATOR.join(self.identifier_to_tuple(namespace))
        response = requests.get(f"{self._get_host()}{Endpoints.list_tables.format(namespace=namespace)}", headers=self._headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            code = exc.response.status_code
            error = exc.response.json()["error"]
            if code == 404:
                raise NoSuchTableError(error["message"])
        response = ListTablesResponse(**response.json())

        return [(*entry.namespace, entry.name) for entry in response.identifiers]

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        pass

    def drop_table(self, identifier: Union[str, Identifier], purge_requested: bool = False) -> None:
        pass

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        pass

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        pass

    def create_namespace(self, namespace: Union[str, Identifier], properties: Optional[Properties] = None) -> NamespaceResponse:
        url = f"{self._get_host()}{Endpoints.create_namespace}"
        headers = {
            AUTHORIZATION_HEADER: f"{BEARER_PREFIX} {self.token.access_token}",
        }
        payload = {"namespace": self.identifier_to_tuple(namespace), "properties": properties}
        response = requests.post(url, json=payload, headers=headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            code = exc.response.status_code
            error = exc.response.json()["error"]
            if code == 409:
                raise AlreadyExistsError(error["message"])
        return NamespaceResponse(**response.json())

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace = NAMESPACE_SEPARATOR.join(self.identifier_to_tuple(namespace))
        url = f"{self._get_host()}{Endpoints.drop_namespace.format(namespace=namespace)}"
        headers = {AUTHORIZATION_HEADER: f"{BEARER_PREFIX} {self.token.access_token}"}
        response = requests.delete(url, headers=headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            code = exc.response.status_code
            error = exc.response.json()["error"]
            if code == 404:
                raise NoSuchNamespaceError(error["message"])

    def list_namespaces(self) -> List[Identifier]:
        url = f"{self._get_host()}{Endpoints.list_namespaces}"
        headers = {AUTHORIZATION_HEADER: f"{BEARER_PREFIX} {self.token.access_token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        namespaces = ListNamespaceResponse(**response.json())
        return namespaces.namespaces

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        namespace = NAMESPACE_SEPARATOR.join(self.identifier_to_tuple(namespace))
        url = f"{self._get_host()}{Endpoints.load_namespace_metadata.format(namespace=namespace)}"
        headers = {AUTHORIZATION_HEADER: f"{BEARER_PREFIX} {self.token.access_token}"}
        response = requests.get(url, headers=headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            code = exc.response.status_code
            error = exc.response.json()["error"]
            if code == 404:
                raise NoSuchNamespaceError(error["message"])

        return NamespaceResponse(**response.json()).properties

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Optional[Properties] = None
    ) -> UpdateNamespacePropertiesResponse:
        namespace = NAMESPACE_SEPARATOR.join(self.identifier_to_tuple(namespace))
        url = f"{self._get_host()}{Endpoints.update_properties.format(namespace=namespace)}"
        headers = {AUTHORIZATION_HEADER: f"{BEARER_PREFIX} {self.token.access_token}"}
        payload = {"removals": list(removals), "updates": updates}
        response = requests.post(url, json=payload, headers=headers)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            code = exc.response.status_code
            error = exc.response.json()["error"]
            if code == 404:
                raise NoSuchNamespaceError(error["message"])
        return UpdateNamespacePropertiesResponse(**response.json())
