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

package org.apache.iceberg.spark;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class HadoopDelegatedCatalog implements Catalog, SupportsNamespaces {

  private final Catalog delegate;
  private final HadoopTables tables;

  public HadoopDelegatedCatalog(Catalog delegate, Configuration conf) {
    this.delegate = delegate;
    this.tables = new HadoopTables(conf);
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return delegate.listTables(namespace);
  }

  @Override
  public Table createTable(TableIdentifier identifier,
                           Schema schema,
                           PartitionSpec spec,
                           String location,
                           Map<String, String> properties) {
    return buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties)
        .create();
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier,
                                               Schema schema,
                                               PartitionSpec spec,
                                               String location,
                                               Map<String, String> properties) {
    return buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties)
        .createTransaction();
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier,
                                                Schema schema,
                                                PartitionSpec spec,
                                                String location,
                                                Map<String, String> properties,
                                                boolean orCreate) {
    TableBuilder tableBuilder = buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(properties);

    if (orCreate) {
      return tableBuilder.createOrReplaceTransaction();
    } else {
      return tableBuilder.replaceTransaction();
    }
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return delegate.tableExists(identifier);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return isHadoopTable(identifier) ? tables.dropTable(identifier.name(), purge) :
        delegate.dropTable(identifier, purge);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (isHadoopTable(from) || isHadoopTable(to)) {
      // todo throw
    } else {
      delegate.renameTable(from, to);
    }
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    return isHadoopTable(identifier) ? tables.load(identifier.name()) : delegate.loadTable(identifier);
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    if (isHadoopTable(identifier)) {
      return new BaseMetastoreCatalogTableBuilder(identifier, schema);
    } else {
      return delegate.buildTable(identifier, schema);
    }
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    delegate.initialize(name, properties);
  }

  // todo I really don't like this check. fragile.
  protected static boolean isHadoopTable(String location) {
    return location.contains("/");
  }

  protected static boolean isHadoopTable(TableIdentifier identifier) {
    return isHadoopTable(identifier.name());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    delegateAsSupportsNamespace(delegate).ifPresent(x -> x.createNamespace(namespace, metadata));
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return delegateAsSupportsNamespace(delegate).map(x -> x.listNamespaces(namespace))
        .orElseGet(Collections::emptyList);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    return delegateAsSupportsNamespace(delegate).map(x -> x.loadNamespaceMetadata(namespace))
        .orElseGet(Collections::emptyMap);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return delegateAsSupportsNamespace(delegate).map(x -> x.dropNamespace(namespace)).orElse(false);
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    return delegateAsSupportsNamespace(delegate).map(x -> x.setProperties(namespace, properties)).orElse(false);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    return delegateAsSupportsNamespace(delegate).map(x -> x.removeProperties(namespace, properties))
        .orElse(false);
  }

  public static Optional<SupportsNamespaces> delegateAsSupportsNamespace(Catalog delegate) {
    return delegate instanceof SupportsNamespaces ? Optional.of((SupportsNamespaces) delegate) : Optional.empty();
  }

  protected class BaseMetastoreCatalogTableBuilder implements TableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();

    public BaseMetastoreCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      Preconditions.checkArgument(isHadoopTable(identifier), "Invalid table identifier: %s", identifier);

      this.identifier = identifier;
      this.schema = schema;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        propertiesBuilder.putAll(properties);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      Map<String, String> properties = propertiesBuilder.build();
      return tables.create(schema, spec, sortOrder, properties, identifier.name());
    }

    @Override
    public Transaction createTransaction() {
      Preconditions.checkNotNull(schema, "A table schema is required");
      String location = identifier.name();
      TableOperations ops = newTableOps(location);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists at location: %s", location);
      }
      Map<String, String> tableProps = propertiesBuilder.build();
      PartitionSpec partitionSpec = spec == null ? PartitionSpec.unpartitioned() : spec;
      SortOrder order = this.sortOrder == null ? SortOrder.unsorted() : this.sortOrder;
      TableMetadata metadata = TableMetadata.newTableMetadata(schema, partitionSpec, order, location, tableProps);
      return Transactions.createTableTransaction(identifier.toString(), ops, metadata);
    }

    private static final String METADATA_JSON = "metadata.json";

    TableOperations newTableOps(String location) {
      if (location.contains(METADATA_JSON)) {
        return new StaticTableOperations(location, new HadoopFileIO(tables.getConf()));
      } else {
        return new HadoopTableOperations(new Path(location), new HadoopFileIO(tables.getConf()), tables.getConf());
      }
    }

    @Override
    public Transaction replaceTransaction() {
      return newReplaceTableTransaction(false);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      return newReplaceTableTransaction(true);
    }

    private Transaction newReplaceTableTransaction(boolean orCreate) {
      TableOperations ops = newTableOps(identifier.name());
      if (!orCreate && ops.current() == null) {
        throw new NoSuchTableException("No such table: %s", identifier);
      }

      TableMetadata metadata;
      if (ops.current() != null) {
        String baseLocation = ops.current().location();
        metadata = ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, propertiesBuilder.build());
      } else {
        String baseLocation = identifier.name();
        metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, propertiesBuilder.build());
      }

      if (orCreate) {
        return Transactions.createOrReplaceTableTransaction(identifier.toString(), ops, metadata);
      } else {
        return Transactions.replaceTableTransaction(identifier.toString(), ops, metadata);
      }
    }
  }

}
