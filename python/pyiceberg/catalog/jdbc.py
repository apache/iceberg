import sqlite3
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Union,
)
from urllib.parse import urlparse

import psycopg2 as db
from psycopg2.extras import DictCursor

from pyiceberg.catalog import (
    METADATA_LOCATION,
    PREVIOUS_METADATA_LOCATION,
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
)
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT

JDBC_URI = "uri"

# Catalog tables
CATALOG_TABLE_NAME = "iceberg_tables"
CATALOG_NAME = "catalog_name"
TABLE_NAMESPACE = "table_namespace"
TABLE_NAME = "table_name"

# Catalog SQL statements
CREATE_CATALOG_TABLE = f"CREATE TABLE {CATALOG_TABLE_NAME} ({CATALOG_NAME} VARCHAR(255) NOT NULL, {TABLE_NAMESPACE} VARCHAR(255) NOT NULL, {TABLE_NAME} VARCHAR(255) NOT NULL, {METADATA_LOCATION} VARCHAR(1000), {PREVIOUS_METADATA_LOCATION} VARCHAR(1000), PRIMARY KEY ({CATALOG_NAME}, {TABLE_NAMESPACE}, {TABLE_NAME}))"
LIST_TABLES_SQL = f"SELECT * FROM {CATALOG_TABLE_NAME} WHERE {CATALOG_NAME} = %s AND {TABLE_NAMESPACE} = %s"
GET_TABLE_SQL = f"SELECT * FROM {CATALOG_TABLE_NAME} WHERE {CATALOG_NAME} = %s AND {TABLE_NAMESPACE} = %s AND {TABLE_NAME} = %s"
DROP_TABLE_SQL = f"DELETE FROM {CATALOG_TABLE_NAME} WHERE {CATALOG_NAME} = %s AND {TABLE_NAMESPACE} = %s AND {TABLE_NAME} = %s "
DO_COMMIT_CREATE_TABLE_SQL = f"INSERT INTO {CATALOG_TABLE_NAME} ({CATALOG_NAME}, {TABLE_NAMESPACE} , {TABLE_NAME} , {METADATA_LOCATION}, {PREVIOUS_METADATA_LOCATION}) VALUES (%s,%s,%s,%s,null)"
RENAME_TABLE_SQL = f"UPDATE {CATALOG_TABLE_NAME} SET {TABLE_NAMESPACE} = %s, {TABLE_NAME} = %s WHERE {CATALOG_NAME} = %s AND {TABLE_NAMESPACE} = %s AND {TABLE_NAME} = %s "

GET_NAMESPACE_SQL = (
    f"SELECT {TABLE_NAMESPACE} FROM {CATALOG_TABLE_NAME} WHERE {CATALOG_NAME} = %s AND {TABLE_NAMESPACE} LIKE %s LIMIT 1"
)
LIST_ALL_TABLE_NAMESPACES_SQL = f"SELECT DISTINCT {TABLE_NAMESPACE} FROM {CATALOG_TABLE_NAME} WHERE {CATALOG_NAME} = %s"

# Catalog Namespace Properties
NAMESPACE_EXISTS_PROPERTY = "exists"
NAMESPACE_MINIMAL_PROPERTIES = {NAMESPACE_EXISTS_PROPERTY: "true"}
NAMESPACE_PROPERTIES_TABLE_NAME = "iceberg_namespace_properties"
NAMESPACE_NAME = "namespace"
NAMESPACE_PROPERTY_KEY = "property_key"
NAMESPACE_PROPERTY_VALUE = "property_value"

# Catalog Namespace SQL statements
CREATE_NAMESPACE_PROPERTIES_TABLE = f"CREATE TABLE {NAMESPACE_PROPERTIES_TABLE_NAME} ({CATALOG_NAME} VARCHAR(255) NOT NULL, {NAMESPACE_NAME} VARCHAR(255) NOT NULL, {NAMESPACE_PROPERTY_KEY} VARCHAR(255), {NAMESPACE_PROPERTY_VALUE} VARCHAR(1000), PRIMARY KEY ({CATALOG_NAME}, {NAMESPACE_NAME}, {NAMESPACE_PROPERTY_KEY}))"
INSERT_NAMESPACE_PROPERTIES_SQL = f"INSERT INTO {NAMESPACE_PROPERTIES_TABLE_NAME} ({CATALOG_NAME}, {NAMESPACE_NAME}, {NAMESPACE_PROPERTY_KEY}, {NAMESPACE_PROPERTY_VALUE}) VALUES "
INSERT_PROPERTIES_VALUES_BASE = "(%s,%s,%s,%s)"
LIST_ALL_PROPERTY_NAMESPACES_SQL = (
    f"SELECT DISTINCT {NAMESPACE_NAME} FROM {NAMESPACE_PROPERTIES_TABLE_NAME} WHERE {CATALOG_NAME} = %s"
)
DELETE_NAMESPACE_PROPERTIES_SQL = f"DELETE FROM {NAMESPACE_PROPERTIES_TABLE_NAME} WHERE {CATALOG_NAME} = %s AND {NAMESPACE_NAME} = %s AND {NAMESPACE_PROPERTY_KEY} IN "
DELETE_ALL_NAMESPACE_PROPERTIES_SQL = (
    f"DELETE FROM {NAMESPACE_PROPERTIES_TABLE_NAME} WHERE {CATALOG_NAME} = %s AND {NAMESPACE_NAME} = %s"
)
UPDATE_NAMESPACE_PROPERTIES_START_SQL = f"UPDATE {NAMESPACE_PROPERTIES_TABLE_NAME} SET {NAMESPACE_PROPERTY_VALUE} = CASE"
UPDATE_NAMESPACE_PROPERTIES_END_SQL = f" END WHERE {CATALOG_NAME} = %s AND {NAMESPACE_NAME} = %s AND {NAMESPACE_PROPERTY_KEY} IN "


GET_NAMESPACE_PROPERTIES_SQL = f"SELECT {NAMESPACE_NAME} FROM {NAMESPACE_PROPERTIES_TABLE_NAME} WHERE {CATALOG_NAME} = %s AND {NAMESPACE_NAME} LIKE %s LIMIT 1"
GET_ALL_NAMESPACE_PROPERTIES_SQL = (
    f"SELECT * FROM {NAMESPACE_PROPERTIES_TABLE_NAME} WHERE {CATALOG_NAME} = %s AND {NAMESPACE_NAME} = %s"
)

# Custom SQL not from JDBCCatalog.java
LIST_ALL_NAMESPACES_SQL = f"""
    SELECT {TABLE_NAMESPACE} AS ns FROM {CATALOG_TABLE_NAME}
    WHERE {CATALOG_NAME} = %s
    UNION
    SELECT {NAMESPACE_NAME} AS ns FROM {NAMESPACE_PROPERTIES_TABLE_NAME}
    WHERE {CATALOG_NAME} = %s
"""


def _sqlite(**properties: str) -> Any:
    parsed_uri = urlparse(properties.get("uri"))
    return sqlite3.connect(database=parsed_uri.path, uri=False)


def _postgresql(**properties: str) -> Any:
    parsed_uri = urlparse(properties.get("uri"))

    return db.connect(
        user=parsed_uri.username,
        password=parsed_uri.password,
        dbname=parsed_uri.path[1:],
        host=parsed_uri.hostname,
        port=parsed_uri.port,
    )


SCHEME_TO_DB = {
    "file": _sqlite,
    "postgresql": _postgresql,
}


class JDBCCatalog(Catalog):
    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)

        if not (uri_prop := self.properties.get("uri")):
            raise NoSuchPropertyException("JDBC connection URI is required")
        # Get a database connection for a specific scheme.
        uri = urlparse(uri_prop)
        uri_scheme = str(uri.scheme)
        if uri_scheme not in SCHEME_TO_DB:
            raise ValueError(f"No registered database for scheme: {uri_scheme}")
        self._get_db_connection = SCHEME_TO_DB[uri_scheme]

    def initialize_catalog_tables(self) -> None:
        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor() as curs:
                curs.execute(CREATE_CATALOG_TABLE)
                curs.execute(CREATE_NAMESPACE_PROPERTIES_TABLE)
                conn.commit()

    def _convert_jdbc_to_iceberg(self, jdbc_row: Dict[str, str]) -> Table:
        # Check for expected properties.
        if not (metadata_location := jdbc_row.get(METADATA_LOCATION)):
            raise NoSuchTableError(f"Table property {METADATA_LOCATION} is missing")
        if not (table_namespace := jdbc_row.get(TABLE_NAMESPACE)):
            raise NoSuchTableError(f"Table property {TABLE_NAMESPACE} is missing")
        if not (table_name := jdbc_row.get(TABLE_NAME)):
            raise NoSuchTableError(f"Table property {TABLE_NAME} is missing")

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(table_namespace, table_name),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
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
        """
        Create an Iceberg table.

        Args:
            identifier: Table identifier.
            schema: Table's schema.
            location: Location for the table. Optional Argument.
            partition_spec: PartitionSpec for the table.
            sort_order: SortOrder for the table.
            properties: Table properties that can be a string based dictionary.

        Returns:
            Table: the created table instance.

        Raises:
            AlreadyExistsError: If a table with the name already exists.
            ValueError: If the identifier is invalid, or no path is given to store metadata.

        """
        database_name, table_name = self.identifier_to_database_and_table(identifier)
        if not self._namespace_exists(database_name):
            raise NoSuchNamespaceError(f"Namespace does not exist: {database_name}")

        location = self._resolve_table_location(location, database_name, table_name)
        metadata_location = self._get_metadata_location(location=location)
        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order, properties=properties
        )
        io = load_file_io(properties=self.properties, location=metadata_location)
        self._write_metadata(metadata, io, metadata_location)

        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor() as curs:
                curs.execute(DO_COMMIT_CREATE_TABLE_SQL, (self.name, database_name, table_name, metadata_location))

        return self.load_table(identifier=identifier)

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        """Loads the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except NoSuchTableError'.
        Note: This method doesn't scan data stored in the table.

        Args:
            identifier (str | Identifier): Table identifier.

        Returns:
            Table: the table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist.
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)
        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor(cursor_factory=DictCursor) as curs:
                curs.execute(GET_TABLE_SQL, (self.name, database_name, table_name))
                row = curs.fetchone()
                return self._convert_jdbc_to_iceberg(row)
        # io = load_file_io({**self.properties, **hive_table.parameters}, hive_table.sd.location)

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table.

        Args:
            identifier (str | Identifier): Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist.
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)
        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor() as curs:
                curs.execute(DROP_TABLE_SQL, (self.name, database_name, table_name))
        # TODO: Check if table did not exist

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        """Rename a fully classified table name.

        Args:
            from_identifier (str | Identifier): Existing table identifier.
            to_identifier (str | Identifier): New table identifier.

        Returns:
            Table: the updated table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist.
        """
        from_database_name, from_table_name = self.identifier_to_database_and_table(from_identifier, NoSuchTableError)
        to_database_name, to_table_name = self.identifier_to_database_and_table(to_identifier)
        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor() as curs:
                curs.execute(RENAME_TABLE_SQL, (to_database_name, to_table_name, self.name, from_database_name, from_table_name))
        # TODO:
        # except NoSuchObjectException as e:
        #     raise NoSuchTableError(f"Table does not exist: {from_table_name}") from e
        # except InvalidOperationException as e:
        #     raise NoSuchNamespaceError(f"Database does not exists: {to_database_name}") from e
        return self.load_table(to_identifier)

    def _namespace_exists(self, namespace: str) -> bool:
        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor() as curs:
                curs.execute(GET_NAMESPACE_SQL, (self.name, namespace + "%"))
                if curs.fetchone():
                    return True
                curs.execute(GET_NAMESPACE_PROPERTIES_SQL, (self.name, namespace + "%"))
                if curs.fetchone():
                    return True
        return False

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = NAMESPACE_MINIMAL_PROPERTIES) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier.
            properties (Properties): A string dictionary of properties for the given namespace.

        Raises:
            NamespaceAlreadyExistsError: If a namespace with the given name already exists.
        """
        database_name = self.identifier_to_database(namespace)
        if self._namespace_exists(database_name):
            raise NamespaceAlreadyExistsError(f"Database {database_name} already exists")

        create_properties = properties if properties else NAMESPACE_MINIMAL_PROPERTIES
        sql = INSERT_NAMESPACE_PROPERTIES_SQL + ",".join([INSERT_PROPERTIES_VALUES_BASE] * len(create_properties))
        args = []
        for key, value in create_properties.items():
            args.extend([self.name, database_name, key, value])

        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor() as curs:
                curs.execute(sql, tuple(args))
                conn.commit()

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        """Drop a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
            NamespaceNotEmptyError: If the namespace is not empty.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        if self._namespace_exists(database_name):
            if tables := self.list_tables(database_name):
                raise NamespaceNotEmptyError(f"Database {database_name} is not empty. {len(tables)} tables exist.")

            with self._get_db_connection(**self.properties) as conn:
                with conn.cursor() as curs:
                    curs.execute(DELETE_ALL_NAMESPACE_PROPERTIES_SQL, (self.name, database_name))
                    conn.commit()

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        """List tables under the given namespace in the catalog.

        If namespace not provided, will list all tables in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: list of table identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)

        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor(cursor_factory=DictCursor) as curs:
                curs.execute(LIST_TABLES_SQL, (self.name, database_name))
                return [(row[TABLE_NAMESPACE], row[TABLE_NAME]) for row in curs.fetchall()]

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: a List of namespace identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """
        # Hierarchical namespace is not supported. Return an empty list
        # TODO: Or is it?
        if namespace:
            return []

        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor() as curs:
                curs.execute(LIST_ALL_NAMESPACES_SQL, (self.name, self.name))
                return [self.identifier_to_tuple(namespace_col) for namespace_col in curs.fetchall()]

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        """Get properties for a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier.

        Returns:
            Properties: Properties for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)

        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor(cursor_factory=DictCursor) as curs:
                curs.execute(GET_ALL_NAMESPACE_PROPERTIES_SQL, (self.name, database_name))
                return {row[NAMESPACE_PROPERTY_KEY]: row[NAMESPACE_PROPERTY_VALUE] for row in curs.fetchall()}

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        """Removes provided property keys and updates properties for a namespace.

        Args:
            namespace (str | Identifier): Namespace identifier.
            removals (Set[str]): Set of property keys that need to be removed. Optional Argument.
            updates (Properties): Properties to be updated for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
            ValueError: If removals and updates have overlapping keys.
        """
        database_name = self.identifier_to_database(namespace)
        if not self._namespace_exists(database_name):
            raise NoSuchNamespaceError(f"Database {database_name} does not exists")

        current_properties = self.load_namespace_properties(namespace=namespace)
        properties_update_summary, updated_properties = self._get_updated_props_and_update_summary(
            current_properties=current_properties, removals=removals, updates=updates
        )

        with self._get_db_connection(**self.properties) as conn:
            with conn.cursor() as curs:
                if removals:
                    remove_sql_list = ",".join(["%s"] * len(removals))
                    curs.execute(f"{DELETE_NAMESPACE_PROPERTIES_SQL} ({remove_sql_list})", (self.name, database_name, *removals))

                if updates:
                    # Build UPDATE statement
                    update_sql = UPDATE_NAMESPACE_PROPERTIES_START_SQL
                    update_sql += f" WHEN {NAMESPACE_PROPERTY_KEY} = %s THEN %s" * len(updates)
                    update_sql += UPDATE_NAMESPACE_PROPERTIES_END_SQL
                    update_sql += f"({','.join(['%s']*len(updates))})"

                    # Build UPDATE statement arguments list
                    update_sql_args = []
                    for key, value in updates.items():
                        update_sql_args.extend([key, value])
                    update_sql_args.extend([self.name, database_name, *updates.keys()])

                    curs.execute(update_sql, tuple(update_sql_args))
                conn.commit()
        return properties_update_summary
