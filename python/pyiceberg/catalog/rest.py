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
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from pydantic import Field, ValidationError
from requests import HTTPError, Session

from pyiceberg import __version__
from pyiceberg.catalog import (
    TOKEN,
    URI,
    WAREHOUSE_LOCATION,
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    AuthorizationExpiredError,
    BadRequestError,
    CommitFailedException,
    CommitStateUnknownException,
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
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import (
    CommitTableRequest,
    CommitTableResponse,
    Table,
    TableMetadata,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT, IcebergBaseModel

ICEBERG_REST_SPEC_VERSION = "0.14.1"


class Endpoints:
    get_config: str = "config"
    list_namespaces: str = "namespaces"
    create_namespace: str = "namespaces"
    load_namespace_metadata: str = "namespaces/{namespace}"
    drop_namespace: str = "namespaces/{namespace}"
    update_namespace_properties: str = "namespaces/{namespace}/properties"
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
SEMICOLON = ":"
KEY = "key"
CERT = "cert"
CLIENT = "client"
CA_BUNDLE = "cabundle"
SSL = "ssl"
SIGV4 = "rest.sigv4-enabled"
SIGV4_REGION = "rest.signing-region"
SIGV4_SERVICE = "rest.signing-name"

NAMESPACE_SEPARATOR = b"\x1F".decode("UTF-8")


class TableResponse(IcebergBaseModel):
    metadata_location: str = Field(alias="metadata-location")
    metadata: TableMetadata
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
    error_description: Optional[str] = None
    error_uri: Optional[str] = None


class RestCatalog(Catalog):
    uri: str
    _session: Session

    def __init__(self, name: str, **properties: str):
        """Rest Catalog.

        You either need to provide a client_id and client_secret, or an already valid token.

        Args:
            name: Name to identify the catalog.
            properties: Properties that are passed along to the configuration.
        """
        super().__init__(name, **properties)
        self.uri = properties[URI]
        self._fetch_config()
        self._session = self._create_session()

    def _create_session(self) -> Session:
        """Creates a request session with provided catalog configuration."""
        session = Session()

        # Sets the client side and server side SSL cert verification, if provided as properties.
        if ssl_config := self.properties.get(SSL):
            if ssl_ca_bundle := ssl_config.get(CA_BUNDLE):  # type: ignore
                session.verify = ssl_ca_bundle
            if ssl_client := ssl_config.get(CLIENT):  # type: ignore
                if all(k in ssl_client for k in (CERT, KEY)):
                    session.cert = (ssl_client[CERT], ssl_client[KEY])
                elif ssl_client_cert := ssl_client.get(CERT):
                    session.cert = ssl_client_cert

        # If we have credentials, but not a token, we want to fetch a token
        if TOKEN not in self.properties and CREDENTIAL in self.properties:
            self.properties[TOKEN] = self._fetch_access_token(session, self.properties[CREDENTIAL])

        # Set Auth token for subsequent calls in the session
        if token := self.properties.get(TOKEN):
            session.headers[AUTHORIZATION_HEADER] = f"{BEARER_PREFIX} {token}"

        # Set HTTP headers
        session.headers["Content-type"] = "application/json"
        session.headers["X-Client-Version"] = ICEBERG_REST_SPEC_VERSION
        session.headers["User-Agent"] = f"PyIceberg/{__version__}"

        # Configure SigV4 Request Signing
        if str(self.properties.get(SIGV4, False)).lower() == "true":
            self._init_sigv4(session)

        return session

    def _check_valid_namespace_identifier(self, identifier: Union[str, Identifier]) -> Identifier:
        """The identifier should have at least one element."""
        identifier_tuple = Catalog.identifier_to_tuple(identifier)
        if len(identifier_tuple) < 1:
            raise NoSuchNamespaceError(f"Empty namespace identifier: {identifier}")
        return identifier_tuple

    def url(self, endpoint: str, prefixed: bool = True, **kwargs: Any) -> str:
        """Constructs the endpoint.

        Args:
            endpoint: Resource identifier that points to the REST catalog.
            prefixed: If the prefix return by the config needs to be appended.

        Returns:
            The base url of the rest catalog.
        """
        url = self.uri
        url = url + "v1/" if url.endswith("/") else url + "/v1/"

        if prefixed:
            url += self.properties.get(PREFIX, "")
            url = url if url.endswith("/") else url + "/"

        return url + endpoint.format(**kwargs)

    def _fetch_access_token(self, session: Session, credential: str) -> str:
        if SEMICOLON in credential:
            client_id, client_secret = credential.split(SEMICOLON)
        else:
            client_id, client_secret = None, credential
        data = {GRANT_TYPE: CLIENT_CREDENTIALS, CLIENT_ID: client_id, CLIENT_SECRET: client_secret, SCOPE: CATALOG_SCOPE}
        url = self.url(Endpoints.get_token, prefixed=False)
        # Uses application/x-www-form-urlencoded by default
        response = session.post(url=url, data=data)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {400: OAuthError, 401: OAuthError})

        return TokenResponse(**response.json()).access_token

    def _fetch_config(self) -> None:
        params = {}
        if warehouse_location := self.properties.get(WAREHOUSE_LOCATION):
            params[WAREHOUSE_LOCATION] = warehouse_location

        with self._create_session() as session:
            response = session.get(self.url(Endpoints.get_config, prefixed=False), params=params)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {})
        config_response = ConfigResponse(**response.json())

        config = config_response.defaults
        config.update(self.properties)
        config.update(config_response.overrides)
        self.properties = config

        # Update URI based on overrides
        self.uri = config[URI]

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

    def _handle_non_200_response(self, exc: HTTPError, error_handler: Dict[int, Type[Exception]]) -> None:
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
        except ValidationError as e:
            # In the case we don't have a proper response
            errs = ", ".join(err["msg"] for err in e.errors())
            response = (
                f"RESTError {exc.response.status_code}: Received unexpected JSON Payload: {exc.response.text}, errors: {errs}"
            )

        raise exception(response) from exc

    def _init_sigv4(self, session: Session) -> None:
        from urllib import parse

        import boto3
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from requests import PreparedRequest
        from requests.adapters import HTTPAdapter

        class SigV4Adapter(HTTPAdapter):
            def __init__(self, **properties: str):
                super().__init__()
                self._properties = properties

            def add_headers(self, request: PreparedRequest, **kwargs: Any) -> None:  # pylint: disable=W0613
                boto_session = boto3.Session()
                credentials = boto_session.get_credentials().get_frozen_credentials()
                region = self._properties.get(SIGV4_REGION, boto_session.region_name)
                service = self._properties.get(SIGV4_SERVICE, "execute-api")

                url = str(request.url).split("?")[0]
                query = str(parse.urlsplit(request.url).query)
                params = dict(parse.parse_qsl(query))

                # remove the connection header as it will be updated after signing
                del request.headers["connection"]

                aws_request = AWSRequest(
                    method=request.method, url=url, params=params, data=request.body, headers=dict(request.headers)
                )

                SigV4Auth(credentials, service, region).add_auth(aws_request)
                original_header = request.headers
                signed_headers = aws_request.headers
                relocated_headers = {}

                # relocate headers if there is a conflict with signed headers
                for header, value in original_header.items():
                    if header in signed_headers and signed_headers[header] != value:
                        relocated_headers[f"Original-{header}"] = value

                request.headers.update(relocated_headers)
                request.headers.update(signed_headers)

        session.mount(self.uri, SigV4Adapter(**self.properties))

    def _response_to_table(self, identifier_tuple: Tuple[str, ...], table_response: TableResponse) -> Table:
        return Table(
            identifier=(self.name,) + identifier_tuple if self.name else identifier_tuple,
            metadata_location=table_response.metadata_location,
            metadata=table_response.metadata,
            io=self._load_file_io(
                {**table_response.metadata.properties, **table_response.config}, table_response.metadata_location
            ),
            catalog=self,
        )

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
        serialized_json = request.model_dump_json().encode("utf-8")
        response = self._session.post(
            self.url(Endpoints.create_table, namespace=namespace_and_table["namespace"]),
            data=serialized_json,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {409: TableAlreadyExistsError})

        table_response = TableResponse(**response.json())
        return self._response_to_table(self.identifier_to_tuple(identifier), table_response)

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        namespace_concat = NAMESPACE_SEPARATOR.join(namespace_tuple)
        response = self._session.get(self.url(Endpoints.list_tables, namespace=namespace_concat))
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError})
        return [(*table.namespace, table.name) for table in ListTablesResponse(**response.json()).identifiers]

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        identifier_tuple = self.identifier_to_tuple(identifier)

        if len(identifier_tuple) <= 1:
            raise NoSuchTableError(f"Missing namespace or invalid identifier: {identifier}")

        response = self._session.get(self.url(Endpoints.load_table, prefixed=True, **self._split_identifier_for_path(identifier)))
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError})

        table_response = TableResponse(**response.json())
        return self._response_to_table(identifier_tuple, table_response)

    def drop_table(self, identifier: Union[str, Identifier], purge_requested: bool = False) -> None:
        response = self._session.delete(
            self.url(Endpoints.drop_table, prefixed=True, purge=purge_requested, **self._split_identifier_for_path(identifier)),
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError})

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self.drop_table(identifier=identifier, purge_requested=True)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        payload = {
            "source": self._split_identifier_for_json(from_identifier),
            "destination": self._split_identifier_for_json(to_identifier),
        }
        response = self._session.post(self.url(Endpoints.rename_table), json=payload)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchTableError, 409: TableAlreadyExistsError})

        return self.load_table(to_identifier)

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        """Updates the table.

        Args:
            table_request (CommitTableRequest): The table requests to be carried out.

        Returns:
            CommitTableResponse: The updated metadata.

        Raises:
            NoSuchTableError: If a table with the given identifier does not exist.
        """
        response = self._session.post(
            self.url(Endpoints.update_table, prefixed=True, **self._split_identifier_for_path(table_request.identifier)),
            data=table_request.model_dump_json().encode("utf-8"),
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(
                exc,
                {
                    409: CommitFailedException,
                    500: CommitStateUnknownException,
                    502: CommitStateUnknownException,
                    504: CommitStateUnknownException,
                },
            )
        return CommitTableResponse(**response.json())

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        payload = {"namespace": namespace_tuple, "properties": properties}
        response = self._session.post(self.url(Endpoints.create_namespace), json=payload)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError, 409: NamespaceAlreadyExistsError})

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        namespace = NAMESPACE_SEPARATOR.join(namespace_tuple)
        response = self._session.delete(self.url(Endpoints.drop_namespace, namespace=namespace))
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {404: NoSuchNamespaceError})

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        namespace_tuple = self.identifier_to_tuple(namespace)
        response = self._session.get(
            self.url(
                f"{Endpoints.list_namespaces}?parent={NAMESPACE_SEPARATOR.join(namespace_tuple)}"
                if namespace_tuple
                else Endpoints.list_namespaces
            ),
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._handle_non_200_response(exc, {})

        namespaces = ListNamespaceResponse(**response.json())
        return [namespace_tuple + child_namespace for child_namespace in namespaces.namespaces]

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        namespace_tuple = self._check_valid_namespace_identifier(namespace)
        namespace = NAMESPACE_SEPARATOR.join(namespace_tuple)
        response = self._session.get(self.url(Endpoints.load_namespace_metadata, namespace=namespace))
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
        response = self._session.post(self.url(Endpoints.update_namespace_properties, namespace=namespace), json=payload)
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
