/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class CachingCatalog implements Catalog {
  public static Catalog wrap(Catalog catalog) {
    return wrap(catalog, true);
  }

  public static Catalog wrap(Catalog catalog, boolean caseSensitive) {
    return new CachingCatalog(catalog, caseSensitive);
  }

  private final Cache<TableIdentifier, Table> tableCache = Caffeine.newBuilder().softValues().build();
  private final Catalog catalog;
  private final boolean caseSensitive;

  private CachingCatalog(Catalog catalog, boolean caseSensitive) {
    this.catalog = catalog;
    this.caseSensitive = caseSensitive;
  }

  private TableIdentifier canonicalizeIdentifier(TableIdentifier tableIdentifier) {
    if (caseSensitive) {
      return tableIdentifier;
    } else {
      return tableIdentifier.toLowerCase();
    }
  }

  @Override
  public String name() {
    return catalog.name();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return catalog.listTables(namespace);
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    TableIdentifier canonicalized = canonicalizeIdentifier(ident);
    Table cached = tableCache.getIfPresent(canonicalized);
    if (cached != null) {
      return cached;
    }

    if (MetadataTableUtils.hasMetadataTableName(canonicalized)) {
      TableIdentifier originTableIdentifier = TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable = tableCache.get(originTableIdentifier, catalog::loadTable);

      // share TableOperations instance of origin table for all metadata tables, so that metadata table instances are
      // also refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations) {
        TableOperations ops = ((HasTableOperations) originTable).operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());

        Table metadataTable = MetadataTableUtils.createMetadataTableInstance(
            ops, catalog.name(), originTableIdentifier,
            canonicalized, type);
        tableCache.put(canonicalized, metadataTable);
        return metadataTable;
      }
    }

    return tableCache.get(canonicalized, catalog::loadTable);
  }

  @Override
  public Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec, String location,
                           Map<String, String> properties) {
    return buildTable(ident, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties)
        .create();
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier ident, Schema schema, PartitionSpec spec,
                                               String location, Map<String, String> properties) {
    return buildTable(ident, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties)
        .createTransaction();
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier ident, Schema schema, PartitionSpec spec,
                                                String location, Map<String, String> properties, boolean orCreate) {
    TableBuilder builder = buildTable(ident, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties);
    if (orCreate) {
      return builder.createOrReplaceTransaction();
    } else {
      return builder.replaceTransaction();
    }
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    boolean dropped = catalog.dropTable(ident, purge);
    invalidate(canonicalizeIdentifier(ident));
    return dropped;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    catalog.renameTable(from, to);
    invalidate(canonicalizeIdentifier(from));
  }

  private void invalidate(TableIdentifier ident) {
    tableCache.invalidate(ident);
    tableCache.invalidateAll(metadataTableIdentifiers(ident));
  }

  private Iterable<TableIdentifier> metadataTableIdentifiers(TableIdentifier ident) {
    ImmutableList.Builder<TableIdentifier> builder = ImmutableList.builder();

    for (MetadataTableType type : MetadataTableType.values()) {
      // metadata table resolution is case insensitive right now
      builder.add(TableIdentifier.parse(ident + "." + type.name()));
      builder.add(TableIdentifier.parse(ident + "." + type.name().toLowerCase(Locale.ROOT)));
    }

    return builder.build();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new CachingTableBuilder(identifier, schema);
  }

  private class CachingTableBuilder implements TableBuilder {
    private final TableIdentifier ident;
    private final TableBuilder innerBuilder;

    private CachingTableBuilder(TableIdentifier identifier, Schema schema) {
      this.innerBuilder = catalog.buildTable(identifier, schema);
      this.ident = identifier;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec spec) {
      innerBuilder.withPartitionSpec(spec);
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder sortOrder) {
      innerBuilder.withSortOrder(sortOrder);
      return this;
    }

    @Override
    public TableBuilder withLocation(String location) {
      innerBuilder.withLocation(location);
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      innerBuilder.withProperties(properties);
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      innerBuilder.withProperty(key, value);
      return this;
    }

    @Override
    public Table create() {
      AtomicBoolean created = new AtomicBoolean(false);
      Table table = tableCache.get(canonicalizeIdentifier(ident), identifier -> {
        created.set(true);
        return innerBuilder.create();
      });

      if (!created.get()) {
        throw new AlreadyExistsException("Table already exists: %s", ident);
      }

      return table;
    }

    @Override
    public Transaction createTransaction() {
      // create a new transaction without altering the cache. the table doesn't exist until the transaction is
      // committed. if the table is created before the transaction commits, any cached version is correct and the
      // transaction create will fail. if the transaction commits before another create, then the cache will be empty.
      return innerBuilder.createTransaction();
    }

    @Override
    public Transaction replaceTransaction() {
      // create a new transaction without altering the cache. the table doesn't change until the transaction is
      // committed. when the transaction commits, invalidate the table in the cache if it is present.
      return CommitCallbackTransaction.addCallback(
          innerBuilder.replaceTransaction(),
          () -> invalidate(canonicalizeIdentifier(ident)));
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      // create a new transaction without altering the cache. the table doesn't change until the transaction is
      // committed. when the transaction commits, invalidate the table in the cache if it is present.
      return CommitCallbackTransaction.addCallback(
          innerBuilder.createOrReplaceTransaction(),
          () -> invalidate(canonicalizeIdentifier(ident)));
    }
  }
}
