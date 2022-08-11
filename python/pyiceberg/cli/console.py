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
# pylint: disable=broad-except
import os
from typing import Literal, Optional, Tuple

import click
from click import Context

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.cli.output import ConsoleOutput, JsonOutput, Output
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError


@click.group()
@click.option("--catalog", default="default")
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
@click.option("--uri")
@click.option("--credential")
@click.pass_context
def run(ctx: Context, catalog: str, output: str, uri: Optional[str], credential: Optional[str]):
    uri_env_var = f"PYICEBERG__CATALOG_{catalog.upper()}_URI"
    credential_env_var = f"PYICEBERG__CATALOG_{catalog.upper()}_CREDENTIAL"

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
            ValueError(
                f"Missing uri. Please provide using --uri or using environment variable PYICEBERG__CATALOG_{catalog.upper()}_URI"
            )
        )
        ctx.exit(1)

    assert uri  # for mypy

    if uri.startswith("http"):
        ctx.obj["catalog"] = RestCatalog(name=catalog, properties={}, uri=uri, credential=credential)
    elif uri.startswith("thrift"):
        ctx.obj["catalog"] = HiveCatalog(name=catalog, properties={}, uri=uri)
    else:
        ctx.obj["output"].exception(
            ValueError("Could not determine catalog type from uri. REST (http/https) and Hive (thrift) is supported")
        )
        ctx.exit(1)


def _get_catalog_and_output(ctx: Context) -> Tuple[Catalog, Output]:
    """
    Small helper to set the types
    """
    return ctx.obj["catalog"], ctx.obj["output"]


@run.command()
@click.pass_context
@click.argument("parent", required=False)
def list(ctx: Context, parent: Optional[str]):  # pylint: disable=redefined-builtin
    catalog, output = _get_catalog_and_output(ctx)

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
    """Describes a table"""
    catalog, output = _get_catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    if len(identifier_tuple) > 0:
        try:
            namespace_properties = catalog.load_namespace_properties(identifier_tuple)
            output.describe_properties(namespace_properties)
            ctx.exit(0)
        except NoSuchNamespaceError as exc:
            if entity != "any":
                output.exception(exc)
                ctx.exit(1)

    if len(identifier_tuple) > 1:
        try:
            catalog_table = catalog.load_table(identifier)
            output.describe_table(catalog_table)
            ctx.exit(0)
        except NoSuchNamespaceError as exc:
            output.exception(exc)
            ctx.exit(1)

    output.exception(NoSuchTableError(f"Expected a namespace or table, got: {identifier}"))
    ctx.exit(1)


@run.command()
@click.option("--entity", type=click.Choice(["any", "namespace", "table"]), default="any")
@click.argument("identifier")
@click.pass_context
def properties(ctx: Context, entity: Literal["name", "namespace", "table"], identifier: str):
    """Fetches the properties from the namespace/table"""
    catalog, output = _get_catalog_and_output(ctx)
    identifier_tuple = Catalog.identifier_to_tuple(identifier)

    try:
        namespace_properties = catalog.load_namespace_properties(identifier_tuple)
        output.describe_properties(namespace_properties)
        ctx.exit(0)
    except NoSuchNamespaceError as exc:
        if entity != "any":
            output.exception(exc)
            ctx.exit(1)

    # We expect a namespace
    if len(identifier_tuple) > 1:
        try:
            metadata = catalog.load_table(identifier_tuple).metadata
            assert metadata
            output.describe_properties(metadata.properties)
            ctx.exit(0)
        except NoSuchTableError as exc:
            if entity != "any":
                output.exception(exc)
                ctx.exit(1)

    output.exception(NoSuchNamespaceError(f"Could not find table/namespace with identifier: {identifier}"))
    ctx.exit(1)


@run.command()
@click.argument("identifier")
@click.pass_context
def schema(ctx: Context, identifier: str):
    catalog, output = _get_catalog_and_output(ctx)

    try:
        metadata = catalog.load_table(identifier).metadata
        assert metadata
        output.schema(metadata.current_schema())
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.command()
@click.argument("identifier")
@click.pass_context
def spec(ctx: Context, identifier: str):
    catalog, output = _get_catalog_and_output(ctx)
    try:
        metadata = catalog.load_table(identifier).metadata
        assert metadata
        output.spec(metadata.current_partition_spec())
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.command()
@click.argument("identifier")
@click.pass_context
def uuid(ctx: Context, identifier: str):
    catalog, output = _get_catalog_and_output(ctx)
    try:
        metadata = catalog.load_table(identifier).metadata
        assert metadata
        output.uuid(metadata.table_uuid)
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.command()
@click.argument("identifier")
@click.pass_context
def location(ctx: Context, identifier: str):
    catalog, output = _get_catalog_and_output(ctx)
    try:
        metadata = catalog.load_table(identifier).metadata
        assert metadata
        output.text(metadata.location)
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@run.group()
def drop(_: Context):
    pass


@drop.command()
@click.argument("identifier")
@click.pass_context
def table(ctx: Context, identifier: str):
    """Drop table"""
    catalog, output = _get_catalog_and_output(ctx)

    try:
        catalog.drop_namespace(identifier)
        output.text(f"Dropped table: {identifier}")
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


@drop.command()
@click.argument("identifier")
@click.pass_context
def namespace(ctx, identifier: str):
    """Drop namespace"""
    catalog, output = _get_catalog_and_output(ctx)

    try:
        catalog.drop_namespace(identifier)
        output.text(f"Dropped namespace: {identifier}")
    except Exception as exc:
        output.exception(exc)
        ctx.exit(1)


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
        ctx.exit(1)


@run.command()
@click.argument("from_identifier")
@click.argument("to_identifier")
@click.pass_context
def rename(ctx, from_identifier: str, to_identifier: str):
    """Rename table"""
    catalog, output = _get_catalog_and_output(ctx)

    try:
        catalog.rename_table(from_identifier, to_identifier)
        output.text(f"Renamed table: {table}")
    except Exception as exc:
        output.exception(exc)
