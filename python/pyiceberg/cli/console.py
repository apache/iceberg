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

import click
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError, NoSuchTableError


@click.group()
@click.option("--catalog", type=click.Choice(["rest"], case_sensitive=False), default="rest")
@click.option("--uri", default=lambda: os.environ.get("PYICEBERG_URI"))
@click.option("--credentials", default=lambda: os.environ.get("PYICEBERG_CREDENTIALS"))
@click.pass_context
def run(ctx, catalog, uri, credentials):
    if catalog == "rest":
        ctx.ensure_object(dict)
        ctx.obj["catalog"] = RestCatalog(name="catalog", properties={}, uri=uri, credentials=credentials)


@run.command()
@click.pass_context
def list_namespace(ctx):
    # Add parent namespaces
    catalog: RestCatalog = ctx.obj["catalog"]
    identifiers = catalog.list_namespaces()

    table = Table(title="Namespaces")

    table.add_column("Namespace")

    for identifier in identifiers:
        table.add_row(".".join(identifier))

    console = Console()
    console.print(table)


@run.command()
@click.argument("namespace")
@click.pass_context
def load_namespace(ctx, namespace: str):
    catalog: RestCatalog = ctx.obj["catalog"]
    console = Console()

    try:
        properties = catalog.load_namespace_properties(namespace)

        if properties:
            table = Table(title=f"{namespace} properties", show_header=False)
            for key, value in properties.items():
                table.add_row(key, value)
            console.print(table)
        else:
            console.print("No properties...")
    except NoSuchNamespaceError:
        console.print(f"Namespace does not exists: {namespace}")


@run.command()
@click.argument("namespace")
@click.pass_context
def drop_namespace(ctx, namespace: str):
    catalog: RestCatalog = ctx.obj["catalog"]
    console = Console()

    try:
        catalog.drop_namespace(namespace)
        console.print(f"Namespace {namespace} has been dropped")
    except NoSuchNamespaceError:
        console.print(f"Namespace does not exists: {namespace}")
    except NamespaceNotEmptyError:
        console.print(f"Namespace is not empty: {namespace}")


@run.command()
@click.argument("namespace")
@click.pass_context
def list_tables(ctx, namespace: str):
    catalog: RestCatalog = ctx.obj["catalog"]
    console = Console()
    try:
        identifiers = catalog.list_tables(namespace)

        table = Table(title="Tables")

        table.add_column("Table name")

        for identifier in identifiers:
            table.add_row(".".join(identifier))

        console.print(table)
    except NoSuchNamespaceError:
        console.print(f"Namespace does not exists: {namespace}")


@run.command()
@click.argument("table")
@click.pass_context
def load_table(ctx, table: str):
    catalog: RestCatalog = ctx.obj["catalog"]
    console = Console()

    try:
        tbl = catalog.load_table(table)
        metadata = tbl.metadata
        table_properties = Table(show_header=False)
        for key, value in metadata.properties.items():
            table_properties.add_row(key, value)

        schema_tree = Tree("Schema")
        for field in metadata.current_schema().fields:
            schema_tree.add(str(field))

        snapshot_tree = Tree("Snapshots")
        for snapshot in metadata.snapshots:
            snapshot_tree.add(f"Snapshot {snapshot.schema_id}: {snapshot.manifest_list}")
            # Add the manifest entries

        output_table = Table(title=table, show_header=False)
        output_table.add_row("Table format version", str(metadata.format_version))
        output_table.add_row("Metadata location", tbl.metadata_location)
        output_table.add_row("Table UUID", str(tbl.metadata.table_uuid))
        output_table.add_row("Last Updated", str(metadata.last_updated_ms))
        output_table.add_row("Partition spec", str(metadata.current_partition_spec()))
        output_table.add_row("Sort order", str(metadata.current_sort_order()))
        output_table.add_row("Schema", schema_tree)
        output_table.add_row("Snapshots", snapshot_tree)
        output_table.add_row("Properties", table_properties)
        console.print(output_table)
    except NoSuchNamespaceError:
        console.print(f"Namespace does not exists: {table}")
    except NoSuchTableError:
        console.print(f"Table does not exists: {table}")


@run.command()
@click.argument("table")
@click.pass_context
def drop_table(ctx, table: str):
    catalog: RestCatalog = ctx.obj["catalog"]
    console = Console()

    try:
        catalog.drop_table(table)
        console.print(f"Table {table} has been dropped")
    except NoSuchNamespaceError:
        console.print(f"Namespace does not exists: {table}")
    except NoSuchTableError:
        console.print(f"Table does not exists: {table}")


@run.command()
@click.argument("from_table")
@click.argument("to_table")
@click.pass_context
def rename_table(ctx, from_table: str, to_table: str):
    catalog: RestCatalog = ctx.obj["catalog"]
    console = Console()

    try:
        catalog.rename_table(from_table, to_table)
        console.print(f"Table {from_table} has been renamed to {to_table}")
    except NoSuchTableError:
        console.print(f"Source table does not exists: {from_table}")
    except NoSuchNamespaceError:
        console.print(f"Destination namespace does not exists: {to_table}")
