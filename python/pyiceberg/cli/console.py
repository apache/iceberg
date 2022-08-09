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
import os
from typing import Literal, Optional, Tuple

import click

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.cli.output import ConsoleOutput, JsonOutput, Output
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError


@click.group()
@click.option("--catalog", default="catalog")
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
@click.option("--uri", default=lambda: os.environ.get("PYICEBERG_URI"))
@click.option("--credential", default=lambda: os.environ.get("PYICEBERG_CREDENTIAL"))
@click.pass_context
def run(ctx, catalog: str, output: str, uri: str, credential: str):
    ctx.ensure_object(dict)
    if output == "text":
        ctx.obj["output"] = ConsoleOutput()
    elif output == "json":
        ctx.obj["output"] = JsonOutput()
    else:
        raise ValueError(f"Unknown output: {output}")

    if not uri:
        ctx.obj["output"].exception(
            f"Missing uri, cannot connect to catalog. Please provide using --uri or by setting the environment variable PYICEBERG_URI"
        )

    if uri.startswith("http"):
        ctx.obj["catalog"] = RestCatalog(name=catalog, properties={}, uri=uri, credential=credential)
    elif uri.startswith("thrift"):
        ctx.obj["catalog"] = HiveCatalog(name=catalog, properties={}, uri=uri)
    else:
        raise ValueError("Could not determine catalog type from uri. REST (http/https) and Hive (thrift) is supported")


def _get_catalog_and_output(ctx) -> Tuple[Catalog, Output]:
    """
    Small helper to set the types
    """
    return ctx.obj["catalog"], ctx.obj["output"]


@run.command()
@click.pass_context
@click.argument("parent", required=False)
def list(ctx, parent: Optional[str]):
    catalog, output = _get_catalog_and_output(ctx)

    # still wip, will become more beautiful
    # https://github.com/apache/iceberg/pull/5467/
    # has been merged
    if parent:
        identifiers = catalog.list_tables(parent)
    else:
        identifiers = catalog.list_namespaces()

    output.identifiers(identifiers)


@run.command()
@click.argument("identifier")
@click.pass_context
def describe(ctx, identifier: str):
    """Describes a table"""
    catalog, output = _get_catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    if len(identifier_tuple) > 1:
        try:
            table = catalog.load_table(identifier)
            output.describe_table(table)
        except NoSuchNamespaceError as exc:
            output.exception(exc)
    else:
        output.exception(NoSuchTableError(f"Expected a namespace, got: {identifier}"))


@run.command()
@click.option("--type", type=click.Choice(["any", "namespace", "table"]), default="any")
@click.argument("identifier")
@click.pass_context
def properties(ctx, type: Literal["name", "namespace", "table"], identifier: str):
    """Fetches the properties from the namespace/table"""
    catalog, output = _get_catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    found_namespace = False
    try:
        namespace_properties = catalog.load_namespace_properties(identifier_tuple)
        output.describe_properties(namespace_properties)
        found_namespace = True
    except NoSuchNamespaceError as exc:
        if type != "any":
            output.exception(exc)

    found_table = False
    # We expect a namespace
    if len(identifier_tuple) > 1:
        try:
            table = catalog.load_table(identifier_tuple)
            assert table.metadata
            output.describe_properties(table.metadata.properties)
            found_table = True
        except NoSuchTableError as exc:
            if type != "any":
                output.exception(exc)

    if type == "any" and not found_namespace and not found_table:
        output.exception(NoSuchNamespaceError(f"Could not find table/namespace with identifier {identifier}"))


@run.command()
@click.argument("identifier")
@click.pass_context
def schema(ctx, identifier: str):
    catalog, output = _get_catalog_and_output(ctx)
    metadata = catalog.load_table(identifier).metadata
    output.schema(metadata.current_schema())


@run.command()
@click.argument("identifier")
@click.pass_context
def spec(ctx, identifier: str):
    catalog, output = _get_catalog_and_output(ctx)
    metadata = catalog.load_table(identifier).metadata
    output.spec(metadata.current_partition_spec())


@run.command()
@click.argument("identifier")
@click.pass_context
def uuid(ctx, identifier: str):
    catalog, output = _get_catalog_and_output(ctx)
    metadata = catalog.load_table(identifier).metadata
    output.uuid(metadata.table_uuid)


@run.command()
@click.argument("identifier")
@click.pass_context
def location(ctx, identifier: str):
    catalog, output = _get_catalog_and_output(ctx)
    metadata = catalog.load_table(identifier).metadata
    output.text(metadata.location)


@run.group()
def drop(ctx):
    pass


@drop.command()
@click.argument("table")
@click.pass_context
def table(ctx, table: str):
    """Drop table"""
    catalog, output = _get_catalog_and_output(ctx)

    try:
        catalog.drop_namespace(table)
        output.text(f"Dropped table: {table}")
    except Exception as exc:
        output.exception(exc)


@drop.command()
@click.argument("namespace")
@click.pass_context
def namespace(ctx, namespace: str):
    """Drop namespace"""
    catalog, output = _get_catalog_and_output(ctx)

    try:
        catalog.drop_namespace(namespace)
        output.text(f"Dropped namespace: {namespace}")
    except Exception as exc:
        output.exception(exc)


@run.command()
@click.argument("from_table")
@click.argument("to_table")
@click.pass_context
def rename_table(ctx, from_table: str, to_table: str):
    catalog, output = _get_catalog_and_output(ctx)

    try:
        catalog.rename_table(from_table, to_table)
        output.text(f"Table {from_table} has been renamed to {to_table}")
    except Exception as exc:
        output.exception(exc)


@run.command()
@click.argument("from_identifier")
@click.argument("to_identifier")
@click.pass_context
def table(ctx, from_identifier: str, to_identifier: str):
    """Rename table"""
    catalog, output = _get_catalog_and_output(ctx)

    try:
        catalog.rename_table(from_identifier, to_identifier)
        output.text(f"Renamed table: {table}")
    except Exception as exc:
        output.exception(exc)
