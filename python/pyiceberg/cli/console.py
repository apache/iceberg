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
import os
from typing import (
    Dict,
    Literal,
    Optional,
    Tuple,
    Type,
)

import click
from click import Context

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.cli.output import ConsoleOutput, JsonOutput, Output
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchPropertyException, NoSuchTableError

SUPPORTED_CATALOGS: Dict[str, Type[Catalog]] = {"thrift": HiveCatalog, "http": RestCatalog}


@click.group()
@click.option("--catalog", default=None)
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
@click.option("--uri")
@click.option("--credential")
@click.pass_context
def run(ctx: Context, catalog: Optional[str], output: str, uri: Optional[str], credential: Optional[str]):
    uri_env_var = "PYICEBERG_URI"
    credential_env_var = "PYICEBERG_CREDENTIAL"

    if not uri:
        uri = os.environ.get(uri_env_var)
    if not credential:
        credential = os.environ.get(credential_env_var)

    ctx.ensure_object(dict)
    if output == "text":
        ctx.obj["output"] = ConsoleOutput()
    else:
        ctx.obj["output"] = JsonOutput()

    if not uri:
        ctx.obj["output"].exception(
            ValueError(f"Missing uri. Please provide using --uri or using environment variable {uri_env_var}")
        )
        ctx.exit(1)

    assert uri  # for mypy

    for scheme, catalog_type in SUPPORTED_CATALOGS.items():
        if uri.startswith(scheme):
            ctx.obj["catalog"] = catalog_type(catalog, uri=uri, credential=credential)  # type: ignore
            break

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
def list(ctx: Context, parent: Optional[str]):  # pylint: disable=redefined-builtin
    """Lists tables or namespaces"""
    catalog, output = _catalog_and_output(ctx)

    # still wip, will become more beautiful
    # https://github.com/apache/iceberg/pull/5467/
    # has been merged
    try:
        if parent:
            identifiers = catalog.list_tables(parent)
        else:
            identifiers = catalog.list_namespaces()
        output.identifiers(identifiers)
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.command()
@click.option("--entity", type=click.Choice(["any", "namespace", "table"]), default="any")
@click.argument("identifier")
@click.pass_context
def describe(ctx: Context, entity: Literal["name", "namespace", "table"], identifier: str):
    """Describes a namespace xor table"""
    catalog, output = _catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    if entity in {"namespace", "any"} and len(identifier_tuple) > 0:
        try:
            namespace_properties = catalog.load_namespace_properties(identifier_tuple)
            output.describe_properties(namespace_properties)
            ctx.exit(0)
        except NoSuchNamespaceError as exc:
            if entity != "any":
                output.exception(exc)
                ctx.exit(1)

    if entity in {"table", "any"} and len(identifier_tuple) > 1:
        try:
            catalog_table = catalog.load_table(identifier)
            output.describe_table(catalog_table)
            ctx.exit(0)
        except NoSuchTableError as exc:
            output.exception(exc)
            ctx.exit(1)

    output.exception(NoSuchTableError(f"Table or namespace does not exist: {identifier}"))
    ctx.exit(1)


@run.command()
@click.argument("identifier")
@click.pass_context
def schema(ctx: Context, identifier: str):
    """Gets the schema of the table"""
    catalog, output = _catalog_and_output(ctx)

    try:
        table = catalog.load_table(identifier)
        output.schema(table.schema())
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.command()
@click.argument("identifier")
@click.pass_context
def spec(ctx: Context, identifier: str):
    """Returns the partition spec of the table"""
    catalog, output = _catalog_and_output(ctx)
    try:
        table = catalog.load_table(identifier)
        output.spec(table.spec())
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.command()
@click.argument("identifier")
@click.pass_context
def uuid(ctx: Context, identifier: str):
    """Returns the UUID of the table"""
    catalog, output = _catalog_and_output(ctx)
    try:
        metadata = catalog.load_table(identifier).metadata
        output.uuid(metadata.table_uuid)
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.command()
@click.argument("identifier")
@click.pass_context
def location(ctx: Context, identifier: str):
    """Returns the location of the table"""
    catalog, output = _catalog_and_output(ctx)
    try:
        table = catalog.load_table(identifier)
        output.text(table.location())
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.group()
def drop():
    """Operations to drop a namespace or table"""


@drop.command()
@click.argument("identifier")
@click.pass_context
def table(ctx: Context, identifier: str):  # noqa: F811
    """Drop table"""
    catalog, output = _catalog_and_output(ctx)

    try:
        catalog.drop_table(identifier)
        output.text(f"Dropped table: {identifier}")
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@drop.command()
@click.argument("identifier")
@click.pass_context
def namespace(ctx, identifier: str):
    """Drop namespace"""
    catalog, output = _catalog_and_output(ctx)

    try:
        catalog.drop_namespace(identifier)
        output.text(f"Dropped namespace: {identifier}")
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.command()
@click.argument("from_identifier")
@click.argument("to_identifier")
@click.pass_context
def rename(ctx, from_identifier: str, to_identifier: str):
    """Renames a table"""
    catalog, output = _catalog_and_output(ctx)

    try:
        catalog.rename_table(from_identifier, to_identifier)
        output.text(f"Renamed table from {from_identifier} to {to_identifier}")
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.group()
def properties():
    """Properties on tables/namespaces"""


@properties.command()
@click.option("--entity", type=click.Choice(["any", "namespace", "table"]), default="any")
@click.argument("identifier")
@click.argument("property_name", required=False)
@click.pass_context
def get(ctx: Context, entity: Literal["name", "namespace", "table"], identifier: str, property_name: str):
    """Fetches a property of a namespace or table"""
    catalog, output = _catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    try:
        namespace_properties = catalog.load_namespace_properties(identifier_tuple)

        if property_name:
            if property_value := namespace_properties.get(property_name):
                output.text(property_value)
                ctx.exit(0)
            elif entity != "any":
                output.exception(NoSuchPropertyException(f"Could not find property {property_name} on {entity} {identifier}"))
                ctx.exit(1)
        else:
            output.describe_properties(namespace_properties)
            ctx.exit(0)
    except NoSuchNamespaceError as exc:
        if entity != "any":
            output.exception(exc)
            ctx.exit(1)

    if len(identifier_tuple) > 1:
        try:
            metadata = catalog.load_table(identifier_tuple).metadata
            assert metadata

            if property_name:
                if property_value := metadata.properties.get(property_name):
                    output.text(property_value)
                    ctx.exit(0)
                elif entity != "any":
                    output.exception(NoSuchPropertyException(f"Could not find property {property_name} on {entity} {identifier}"))
                    ctx.exit(1)
            else:
                output.describe_properties(metadata.properties)
                ctx.exit(0)
        except NoSuchTableError as exc:
            if entity != "any":
                output.exception(exc)
                ctx.exit(1)

    property_err = ""
    if property_name:
        property_err = f" with property {property_name}"

    output.exception(NoSuchNamespaceError(f"Table or namespace does not exist: {identifier}{property_err}"))
    ctx.exit(1)


@properties.group()
def set():
    """Removes properties on tables/namespaces"""


@set.command()  # type: ignore
@click.argument("identifier")
@click.argument("property_name")
@click.argument("property_value")
@click.pass_context
def namespace(ctx: Context, identifier: str, property_name: str, property_value: str):  # noqa: F811
    """Sets a property of a namespace or table"""
    catalog, output = _catalog_and_output(ctx)
    try:
        catalog.update_namespace_properties(identifier, updates={property_name: property_value})
        output.text(f"Updated {property_name} on {identifier}")
    except NoSuchNamespaceError as exc:
        output.exception(exc)
        ctx.exit(1)


@set.command()  # type: ignore
@click.argument("identifier")
@click.argument("property_name")
@click.argument("property_value")
@click.pass_context
def table(ctx: Context, identifier: str, property_name: str, property_value: str):  # noqa: F811
    """Sets a property on a table"""
    catalog, output = _catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    try:
        _ = catalog.load_table(identifier_tuple)
        output.text(f"Setting {property_name}={property_value} on {identifier}")
        output.exception(NotImplementedError("Writing is WIP"))
        ctx.exit(1)

    except NoSuchTableError as exc:
        output.exception(exc)
        ctx.exit(1)

    output.exception(NoSuchNamespaceError(f"Could not find table/namespace {identifier} with property {property}"))
    ctx.exit(1)


@properties.group()
def remove():
    """Removes properties on tables/namespaces"""


@remove.command()  # type: ignore
@click.argument("identifier")
@click.argument("property_name")
@click.pass_context
def namespace(ctx: Context, identifier: str, property_name: str):  # noqa: F811
    """Removes a property from a namespace"""
    catalog, output = _catalog_and_output(ctx)
    try:
        result = catalog.update_namespace_properties(identifier, removals={property_name})

        if result.removed == [property_name]:
            output.text(f"Property {property_name} removed from {identifier}")
        else:
            raise NoSuchPropertyException(f"Property {property_name} does not exists on {identifier}")
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@remove.command()  # type: ignore
@click.argument("identifier")
@click.argument("property_name")
@click.pass_context
def table(ctx: Context, identifier: str, property_name: str):  # noqa: F811
    """Removes a property from a table"""
    catalog, output = _catalog_and_output(ctx)
    try:
        table = catalog.load_table(identifier)
        if property_name in table.metadata.properties:
            # We should think of the process here
            # Do we want something similar as in Java:
            # https://github.com/apache/iceberg/blob/master/api/src/main/java/org/apache/iceberg/Table.java#L178
            del table.metadata.properties
            output.exception(NotImplementedError("Writing is WIP"))
            ctx.exit(1)
        else:
            raise NoSuchPropertyException(f"Property {property_name} does not exists on {identifier}")
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)
