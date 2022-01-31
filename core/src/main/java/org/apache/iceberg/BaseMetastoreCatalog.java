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

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMetastoreCatalog implements Catalog {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreCatalog.class);
  private Map<String, String> catalogProps = Collections.emptyMap();

  @Override
  public void initialize(String name, Map<String, String> properties) {
    catalogProps = properties;
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    Table result;
    if (isValidIdentifier(identifier)) {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() == null) {
        // the identifier may be valid for both tables and metadata tables
        if (isValidMetadataIdentifier(identifier)) {
          result = loadMetadataTable(identifier);

        } else {
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        }

      } else {
        result = new BaseTable(ops, fullTableName(name(), identifier));
      }

    } else if (isValidMetadataIdentifier(identifier)) {
      result = loadMetadataTable(identifier);

    } else {
      throw new NoSuchTableException("Invalid table identifier: %s", identifier);
    }

    LOG.info("Table loaded by catalog: {}", result);
    return result;
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new BaseMetastoreCatalogTableBuilder(identifier, schema);
  }

  private Table loadMetadataTable(TableIdentifier identifier) {
    String tableName = identifier.name();
    MetadataTableType type = MetadataTableType.from(tableName);
    if (type != null) {
      TableIdentifier baseTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
      TableOperations ops = newTableOps(baseTableIdentifier);
      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", baseTableIdentifier);
      }

      return MetadataTableUtils.createMetadataTableInstance(ops, name(), baseTableIdentifier, identifier, type);
    } else {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }
  }

  private boolean isValidMetadataIdentifier(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null &&
        isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
  }

  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    // by default allow all identifiers
    return true;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  protected abstract TableOperations newTableOps(TableIdentifier tableIdentifier);

  protected abstract String defaultWarehouseLocation(TableIdentifier tableIdentifier);

  protected class BaseMetastoreCatalogTableBuilder implements TableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;

    public BaseMetastoreCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      Preconditions.checkArgument(isValidIdentifier(identifier), "Invalid table identifier: %s", identifier);

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
      this.location = newLocation;
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
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      Map<String, String> properties = updateTableProperties(propertiesBuilder.build());
      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, properties);

      try {
        ops.commit(null, metadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Table was created concurrently: %s", identifier);
      }

      return new BaseTable(ops, fullTableName(name(), identifier));
    }

    @Override
    public Transaction createTransaction() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      Map<String, String> properties = updateTableProperties(propertiesBuilder.build());
      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, properties);
      return Transactions.createTableTransaction(identifier.toString(), ops, metadata);
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
      TableOperations ops = newTableOps(identifier);
      if (!orCreate && ops.current() == null) {
        throw new NoSuchTableException("No such table: %s", identifier);
      }

      TableMetadata metadata;
      if (ops.current() != null) {
        String baseLocation = location != null ? location : ops.current().location();
        metadata = ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, propertiesBuilder.build());
      } else {
        String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
        Map<String, String> properties = updateTableProperties(propertiesBuilder.build());
        metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, properties);
      }

      if (orCreate) {
        return Transactions.createOrReplaceTableTransaction(identifier.toString(), ops, metadata);
      } else {
        return Transactions.replaceTableTransaction(identifier.toString(), ops, metadata);
      }
    }
  }

  protected static String fullTableName(String catalogName, TableIdentifier identifier) {
    StringBuilder sb = new StringBuilder();

    if (catalogName.contains("/") || catalogName.contains(":")) {
      // use / for URI-like names: thrift://host:port/db.table
      sb.append(catalogName);
      if (!catalogName.endsWith("/")) {
        sb.append("/");
      }
    } else {
      // use . for non-URI named catalogs: prod.db.table
      sb.append(catalogName).append(".");
    }

    for (String level : identifier.namespace().levels()) {
      sb.append(level).append(".");
    }

    sb.append(identifier.name());

    return sb.toString();
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withLocation(location)
        .withProperties(updateTableProperties(properties))
        .create();
  }

  /**
   * Get default table properties set at Catalog level through catalog properties.
   *
   * @return default table properties specified in catalog properties
   */
  private Map<String, String> tableDefaultProperties() {
    Map<String, String> props = catalogProps == null ? Collections.emptyMap() : catalogProps;

    return props.entrySet().stream()
        .filter(e -> e.getKey().toLowerCase(Locale.ROOT).startsWith(CatalogProperties.TABLE_DEFAULT_PREFIX))
        .collect(Collectors.toMap(e -> e.getKey().replace(CatalogProperties.TABLE_DEFAULT_PREFIX, ""),
            Map.Entry::getValue));
  }

  /**
   * Get table properties that are enforced at Catalog level through catalog properties.
   *
   * @return default table properties enforced through catalog properties
   */
  private Map<String, String> tableOverrideProperties() {
    Map<String, String> props = catalogProps == null ? Collections.emptyMap() : catalogProps;

    return props.entrySet().stream()
        .filter(e -> e.getKey().toLowerCase(Locale.ROOT).startsWith(CatalogProperties.TABLE_OVERRIDE_PREFIX))
        .collect(Collectors.toMap(e -> e.getKey().replace(CatalogProperties.TABLE_OVERRIDE_PREFIX, ""),
            Map.Entry::getValue));
  }

  /**
   * Return updated table properties with table properties defaults and enforcements set at Catalog level through
   * catalog properties.
   *
   * @return updated table properties with defaults and enforcements set at Catalog level
   */
  private Map<String, String> updateTableProperties(Map<String, String> tableProperties) {
    Map<String, String> props = tableProperties == null ? Collections.emptyMap() : tableProperties;

    return Stream.concat(
            Stream.concat(
                tableDefaultProperties().entrySet().stream(),
                props.entrySet().stream()),
            tableOverrideProperties().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2));
  }
}
