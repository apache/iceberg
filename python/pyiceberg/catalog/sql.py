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

from typing import (
    List,
    Optional,
    Set,
    Union,
)

from sqlalchemy import (
    String,
    create_engine,
    delete,
    insert,
    select,
    union,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Session,
    mapped_column,
)

from pyiceberg.catalog import (
    METADATA_LOCATION,
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
    TableAlreadyExistsError,
)
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableRequest, CommitTableResponse, Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT


class SqlCatalogBaseTable(MappedAsDataclass, DeclarativeBase):
    pass


class IcebergTables(SqlCatalogBaseTable):
    __tablename__ = "iceberg_tables"

    catalog_name: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    table_namespace: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    metadata_location: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    previous_metadata_location: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)


class IcebergNamespaceProperties(SqlCatalogBaseTable):
    __tablename__ = "iceberg_namespace_properties"
    # Catalog minimum Namespace Properties
    NAMESPACE_MINIMAL_PROPERTIES = {"exists": "true"}

    catalog_name: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    property_key: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    property_value: Mapped[str] = mapped_column(String(1000), nullable=False)


class SqlCatalog(Catalog):
    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)

        if not (uri_prop := self.properties.get("uri")):
            raise NoSuchPropertyException("SQL connection URI is required")
        self.engine = create_engine(uri_prop, echo=True)

    def create_tables(self) -> None:
        SqlCatalogBaseTable.metadata.create_all(self.engine)

    def destroy_tables(self) -> None:
        SqlCatalogBaseTable.metadata.drop_all(self.engine)

    def _convert_orm_to_iceberg(self, orm_table: IcebergTables) -> Table:
        # Check for expected properties.
        if not (metadata_location := orm_table.metadata_location):
            raise NoSuchTableError(f"Table property {METADATA_LOCATION} is missing")
        if not (table_namespace := orm_table.table_namespace):
            raise NoSuchTableError(f"Table property {IcebergTables.table_namespace} is missing")
        if not (table_name := orm_table.table_name):
            raise NoSuchTableError(f"Table property {IcebergTables.table_name} is missing")

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(self.name, table_namespace, table_name),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
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

        with Session(self.engine) as session:
            try:
                session.add(
                    IcebergTables(
                        catalog_name=self.name,
                        table_namespace=database_name,
                        table_name=table_name,
                        metadata_location=metadata_location,
                        previous_metadata_location=None,
                    )
                )
                session.commit()
            except IntegrityError as e:
                raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e

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
        with Session(self.engine) as session:
            stmt = select(IcebergTables).where(
                IcebergTables.catalog_name == self.name,
                IcebergTables.table_namespace == database_name,
                IcebergTables.table_name == table_name,
            )
            result = session.scalar(stmt)
        if result:
            return self._convert_orm_to_iceberg(result)
        raise NoSuchTableError(f"Table does not exist: {database_name}.{table_name}")

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table.

        Args:
            identifier (str | Identifier): Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist.
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)
        with Session(self.engine) as session:
            res = session.execute(
                delete(IcebergTables).where(
                    IcebergTables.catalog_name == self.name,
                    IcebergTables.table_namespace == database_name,
                    IcebergTables.table_name == table_name,
                )
            )
            session.commit()
        if res.rowcount < 1:
            raise NoSuchTableError(f"Table does not exist: {database_name}.{table_name}")

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        """Rename a fully classified table name.

        Args:
            from_identifier (str | Identifier): Existing table identifier.
            to_identifier (str | Identifier): New table identifier.

        Returns:
            Table: the updated table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist.
            TableAlreadyExistsError: If a table with the new name already exist.
            NoSuchNamespaceError: If the target namespace does not exist.
        """
        from_database_name, from_table_name = self.identifier_to_database_and_table(from_identifier, NoSuchTableError)
        to_database_name, to_table_name = self.identifier_to_database_and_table(to_identifier)
        if not self._namespace_exists(to_database_name):
            raise NoSuchNamespaceError(f"Namespace does not exist: {to_database_name}")
        with Session(self.engine) as session:
            try:
                stmt = (
                    update(IcebergTables)
                    .where(
                        IcebergTables.catalog_name == self.name,
                        IcebergTables.table_namespace == from_database_name,
                        IcebergTables.table_name == from_table_name,
                    )
                    .values(table_namespace=to_database_name, table_name=to_table_name)
                )
                result = session.execute(stmt)
                if result.rowcount < 1:
                    raise NoSuchTableError(f"Table does not exist: {from_table_name}")
                session.commit()
            except IntegrityError as e:
                raise TableAlreadyExistsError(f"Table {to_database_name}.{to_table_name} already exists") from e
        return self.load_table(to_identifier)

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        """Updates one or more tables.

        Args:
            table_request (CommitTableRequest): The table requests to be carried out.

        Returns:
            CommitTableResponse: The updated metadata.

        Raises:
            NoSuchTableError: If a table with the given identifier does not exist.
        """
        raise NotImplementedError

    def _namespace_exists(self, identifier: Union[str, Identifier]) -> bool:
        namespace = self.identifier_to_database(identifier)
        with Session(self.engine) as session:
            stmt = (
                select(IcebergTables)
                .where(IcebergTables.catalog_name == self.name, IcebergTables.table_namespace == namespace)
                .limit(1)
            )
            result = session.execute(stmt).all()
            if result:
                return True
            stmt = (
                select(IcebergNamespaceProperties)
                .where(
                    IcebergNamespaceProperties.catalog_name == self.name,
                    IcebergNamespaceProperties.namespace == namespace,
                )
                .limit(1)
            )
            result = session.execute(stmt).all()
            if result:
                return True
        return False

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier.
            properties (Properties): A string dictionary of properties for the given namespace.

        Raises:
            NamespaceAlreadyExistsError: If a namespace with the given name already exists.
        """
        if not properties:
            properties = IcebergNamespaceProperties.NAMESPACE_MINIMAL_PROPERTIES
        database_name = self.identifier_to_database(namespace)
        if self._namespace_exists(database_name):
            raise NamespaceAlreadyExistsError(f"Database {database_name} already exists")

        create_properties = properties if properties else IcebergNamespaceProperties.NAMESPACE_MINIMAL_PROPERTIES
        with Session(self.engine) as session:
            for key, value in create_properties.items():
                session.add(
                    IcebergNamespaceProperties(
                        catalog_name=self.name, namespace=database_name, property_key=key, property_value=value
                    )
                )
            session.commit()

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

            with Session(self.engine) as session:
                session.execute(
                    delete(IcebergNamespaceProperties).where(
                        IcebergNamespaceProperties.catalog_name == self.name,
                        IcebergNamespaceProperties.namespace == database_name,
                    )
                )
                session.commit()

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

        stmt = select(IcebergTables).where(
            IcebergTables.catalog_name == self.name, IcebergTables.table_namespace == database_name
        )
        with Session(self.engine) as session:
            result = session.scalars(stmt)
            return [(table.table_namespace, table.table_name) for table in result]

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: a List of namespace identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist.
        """
        if namespace and not self._namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")

        table_stmt = select(IcebergTables.table_namespace).where(IcebergTables.catalog_name == self.name)
        namespace_stmt = select(IcebergNamespaceProperties.namespace).where(IcebergNamespaceProperties.catalog_name == self.name)
        if namespace:
            database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
            table_stmt = table_stmt.where(IcebergTables.table_namespace.like(database_name))
            namespace_stmt = namespace_stmt.where(IcebergNamespaceProperties.namespace.like(database_name))
        stmt = union(
            table_stmt,
            namespace_stmt,
        )
        with Session(self.engine) as session:
            return [self.identifier_to_tuple(namespace_col) for namespace_col in session.execute(stmt).scalars()]

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

        stmt = select(IcebergNamespaceProperties).where(
            IcebergNamespaceProperties.catalog_name == self.name, IcebergNamespaceProperties.namespace == database_name
        )
        with Session(self.engine) as session:
            result = session.scalars(stmt)
            return {props.property_key: props.property_value for props in result}

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
        properties_update_summary = self._get_updated_props_and_update_summary(
            current_properties=current_properties, removals=removals, updates=updates
        )[0]

        with Session(self.engine) as session:
            if removals:
                delete_stmt = delete(IcebergNamespaceProperties).where(
                    IcebergNamespaceProperties.catalog_name == self.name,
                    IcebergNamespaceProperties.namespace == database_name,
                    IcebergNamespaceProperties.property_key.in_(removals),
                )
                session.execute(delete_stmt)

            if updates:
                # SQLAlchemy does not (yet) support engine agnostic UPSERT
                # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-upsert-statements
                # This is not a problem since it runs in a single transaction
                delete_stmt = delete(IcebergNamespaceProperties).where(
                    IcebergNamespaceProperties.catalog_name == self.name,
                    IcebergNamespaceProperties.namespace == database_name,
                    IcebergNamespaceProperties.property_key.in_(set(updates.keys())),
                )
                session.execute(delete_stmt)
                insert_stmt = insert(IcebergNamespaceProperties)
                for property_key, property_value in updates.items():
                    insert_stmt = insert_stmt.values(
                        catalog_name=self.name, namespace=database_name, property_key=property_key, property_value=property_value
                    )
                session.execute(insert_stmt)
            session.commit()
        return properties_update_summary
