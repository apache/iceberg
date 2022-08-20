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
# pylint: disable=broad-except,redefined-builtin,redefined-outer-name
from functools import wraps
from typing import (
    Dict,
    Literal,
    Optional,
    Tuple,
    Type,
)

import click
from click import Context

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.cli.output import ConsoleOutput, JsonOutput, Output
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchPropertyException, NoSuchTableError

SUPPORTED_CATALOGS: Dict[str, Type[Catalog]] = {"thrift": HiveCatalog, "http": RestCatalog}


def catch_exception():
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                ctx: Context = click.get_current_context(silent=True)
                _, output = _catalog_and_output(ctx)
                output.exception(e)
                ctx.exit(1)

        return wrapper

    return decorator


@click.group()
@click.option("--catalog", default="default")
@click.option("--verbose", type=click.BOOL)
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
@click.option("--uri")
@click.option("--credential")
@click.pass_context
def run(ctx: Context, catalog: str, verbose: bool, output: str, uri: Optional[str], credential: Optional[str]):
    properties = {}
    if uri:
        properties["uri"] = uri
    if credential:
        properties["credential"] = credential

    ctx.ensure_object(dict)
    if output == "text":
        ctx.obj["output"] = ConsoleOutput(verbose=verbose)
    else:
        ctx.obj["output"] = JsonOutput(verbose=verbose)

    try:
        try:
            ctx.obj["catalog"] = load_catalog(catalog, **properties)
        except ValueError as exc:
            raise ValueError(
                f"URI missing, please provide using --uri, the config or environment variable PYICEBERG_CATALOG__{catalog.upper()}__URI"
            ) from exc
    except Exception as e:
        ctx.obj["output"].exception(e)
        ctx.exit(1)

    if not isinstance(ctx.obj["catalog"], Catalog):
        ctx.obj["output"].exception(
            ValueError("Could not determine catalog type from uri. REST (http/https) and Hive (thrift) is supported")
        )
        ctx.exit(1)


def _catalog_and_output(ctx: Context) -> Tuple[Catalog, Output]:
    """
    Small helper to set the types
    """
    return ctx.obj["catalog"], ctx.obj["output"]


@run.command()
@click.pass_context
@click.argument("parent", required=False)
@catch_exception()
def list(ctx: Context, parent: Optional[str]):  # pylint: disable=redefined-builtin
    """Lists tables or namespaces"""
    catalog, output = _catalog_and_output(ctx)

    identifiers = catalog.list_namespaces(parent or ())
    if not identifiers and parent:
        identifiers = catalog.list_tables(parent)
    output.identifiers(identifiers)


@run.command()
@click.option("--entity", type=click.Choice(["any", "namespace", "table"]), default="any")
@click.argument("identifier")
@click.pass_context
@catch_exception()
def describe(ctx: Context, entity: Literal["name", "namespace", "table"], identifier: str):
    """Describes a namespace xor table"""
    catalog, output = _catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    is_namespace = False
    if entity in {"namespace", "any"} and len(identifier_tuple) > 0:
        try:
            namespace_properties = catalog.load_namespace_properties(identifier_tuple)
            output.describe_properties(namespace_properties)
            is_namespace = True
        except NoSuchNamespaceError as exc:
            if entity != "any" or len(identifier_tuple) == 1:  # type: ignore
                raise exc

    is_table = False
    if entity in {"table", "any"} and len(identifier_tuple) > 1:
        try:
            catalog_table = catalog.load_table(identifier)
            output.describe_table(catalog_table)
            is_table = True
        except NoSuchTableError as exc:
            if entity != "any":
                raise exc

    if is_namespace is False and is_table is False:
        raise NoSuchTableError(f"Table or namespace does not exist: {identifier}")


@run.command()
@click.argument("identifier")
@click.pass_context
@catch_exception()
def schema(ctx: Context, identifier: str):
    """Gets the schema of the table"""
    catalog, output = _catalog_and_output(ctx)
    table = catalog.load_table(identifier)
    output.schema(table.schema())


@run.command()
@click.argument("identifier")
@click.pass_context
@catch_exception()
def spec(ctx: Context, identifier: str):
    """Returns the partition spec of the table"""
    catalog, output = _catalog_and_output(ctx)
    table = catalog.load_table(identifier)
    output.spec(table.spec())


@run.command()
@click.argument("identifier")
@click.pass_context
@catch_exception()
def uuid(ctx: Context, identifier: str):
    """Returns the UUID of the table"""
    catalog, output = _catalog_and_output(ctx)
    metadata = catalog.load_table(identifier).metadata
    output.uuid(metadata.table_uuid)


@run.command()
@click.argument("identifier")
@click.pass_context
@catch_exception()
def location(ctx: Context, identifier: str):
    """Returns the location of the table"""
    catalog, output = _catalog_and_output(ctx)
    table = catalog.load_table(identifier)
    output.text(table.location())


@run.group()
def drop():
    """Operations to drop a namespace or table"""


@drop.command()
@click.argument("identifier")
@click.pass_context
@catch_exception()
def table(ctx: Context, identifier: str):  # noqa: F811
    """Drop table"""
    catalog, output = _catalog_and_output(ctx)

    catalog.drop_table(identifier)
    output.text(f"Dropped table: {identifier}")


@drop.command()
@click.argument("identifier")
@click.pass_context
@catch_exception()
def namespace(ctx, identifier: str):
    """Drop namespace"""
    catalog, output = _catalog_and_output(ctx)

    catalog.drop_namespace(identifier)
    output.text(f"Dropped namespace: {identifier}")


@run.command()
@click.argument("from_identifier")
@click.argument("to_identifier")
@click.pass_context
@catch_exception()
def rename(ctx, from_identifier: str, to_identifier: str):
    """Renames a table"""
    catalog, output = _catalog_and_output(ctx)

    catalog.rename_table(from_identifier, to_identifier)
    output.text(f"Renamed table from {from_identifier} to {to_identifier}")


@run.group()
def properties():
    """Properties on tables/namespaces"""


@properties.command()
@click.option("--entity", type=click.Choice(["any", "namespace", "table"]), default="any")
@click.argument("identifier")
@click.argument("property_name", required=False)
@click.pass_context
@catch_exception()
def get(ctx: Context, entity: Literal["name", "namespace", "table"], identifier: str, property_name: str):
    """Fetches a property of a namespace or table"""
    catalog, output = _catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    is_namespace = False
    if entity in {"namespace", "any"}:
        try:
            namespace_properties = catalog.load_namespace_properties(identifier_tuple)

            if property_name:
                if property_value := namespace_properties.get(property_name):
                    output.text(property_value)
                    is_namespace = True
                else:
                    raise NoSuchPropertyException(f"Could not find property {property_name} on namespace {identifier}")
            else:
                output.describe_properties(namespace_properties)
                is_namespace = True
        except NoSuchNamespaceError as exc:
            if entity != "any" or len(identifier_tuple) <= 1:  # type: ignore
                raise exc
    is_table = False
    if is_namespace is False and len(identifier_tuple) > 1 and entity in {"table", "any"}:
        metadata = catalog.load_table(identifier_tuple).metadata
        assert metadata

        if property_name:
            if property_value := metadata.properties.get(property_name):
                output.text(property_value)
                is_table = True
            else:
                raise NoSuchPropertyException(f"Could not find property {property_name} on table {identifier}")
        else:
            output.describe_properties(metadata.properties)
            is_table = True

    if is_namespace is False and is_table is False:
        property_err = ""
        if property_name:
            property_err = f" with property {property_name}"

        raise NoSuchNamespaceError(f"Table or namespace does not exist: {identifier}{property_err}")


@properties.group()
def set():
    """Removes properties on tables/namespaces"""


@set.command()  # type: ignore
@click.argument("identifier")
@click.argument("property_name")
@click.argument("property_value")
@click.pass_context
@catch_exception()
def namespace(ctx: Context, identifier: str, property_name: str, property_value: str):  # noqa: F811
    """Sets a property of a namespace or table"""
    catalog, output = _catalog_and_output(ctx)

    catalog.update_namespace_properties(identifier, updates={property_name: property_value})
    output.text(f"Updated {property_name} on {identifier}")


@set.command()  # type: ignore
@click.argument("identifier")
@click.argument("property_name")
@click.argument("property_value")
@click.pass_context
@catch_exception()
def table(ctx: Context, identifier: str, property_name: str, property_value: str):  # noqa: F811
    """Sets a property on a table"""
    catalog, output = _catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    _ = catalog.load_table(identifier_tuple)
    output.text(f"Setting {property_name}={property_value} on {identifier}")
    raise NotImplementedError("Writing is WIP")


@properties.group()
def remove():
    """Removes properties on tables/namespaces"""


@remove.command()  # type: ignore
@click.argument("identifier")
@click.argument("property_name")
@click.pass_context
@catch_exception()
def namespace(ctx: Context, identifier: str, property_name: str):  # noqa: F811
    """Removes a property from a namespace"""
    catalog, output = _catalog_and_output(ctx)

    result = catalog.update_namespace_properties(identifier, removals={property_name})

    if result.removed == [property_name]:
        output.text(f"Property {property_name} removed from {identifier}")
    else:
        raise NoSuchPropertyException(f"Property {property_name} does not exist on {identifier}")


@remove.command()  # type: ignore
@click.argument("identifier")
@click.argument("property_name")
@click.pass_context
@catch_exception()
def table(ctx: Context, identifier: str, property_name: str):  # noqa: F811
    """Removes a property from a table"""
    catalog, output = _catalog_and_output(ctx)
    table = catalog.load_table(identifier)
    if property_name in table.metadata.properties:
        # We should think of the process here
        # Do we want something similar as in Java:
        # https://github.com/apache/iceberg/blob/master/api/src/main/java/org/apache/iceberg/Table.java#L178
        del table.metadata.properties
        output.exception(NotImplementedError("Writing is WIP"))
        ctx.exit(1)
    else:
        raise NoSuchPropertyException(f"Property {property_name} does not exist on {identifier}")
