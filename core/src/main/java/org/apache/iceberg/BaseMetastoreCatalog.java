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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMetastoreCatalog implements Catalog, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreCatalog.class);

  private MetricsReporter metricsReporter;

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
        result = new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
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
  public Table registerTable(
      TableIdentifier identifier, String metadataFileLocation, boolean overwrite) {
    Preconditions.checkArgument(
        identifier != null && isValidIdentifier(identifier), "Invalid identifier: %s", identifier);
    Preconditions.checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Cannot register an empty metadata file location as a table");

    // Throw an exception if the table already exists in the catalog and overwriting is not
    // requested.
    if (tableExists(identifier) && !overwrite) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    TableOperations ops = newTableOps(identifier);
    TableMetadata newMetadata =
        TableMetadataParser.read(ops.io(), ops.io().newInputFile(metadataFileLocation));
    TableMetadata existing = ops.current();
    if (existing != null && overwrite) {
      if (newMetadata.uuid() != null && existing.uuid() != null) {
        Preconditions.checkArgument(
            newMetadata.uuid().equals(existing.uuid()),
            "Table UUID does not match: current=%s != refreshed=%s",
            existing.uuid(),
            newMetadata.uuid());
      }
      AtomicBoolean isRetry = new AtomicBoolean(false);
      // overwrite new metadata with retry
      Tasks.foreach(ops)
          .retry(COMMIT_NUM_RETRIES_DEFAULT)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
              COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              taskOps -> {
                TableMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();
                isRetry.set(true);

                // commit
                taskOps.commit(base, newMetadata);
              });
    } else {
      ops.commit(null, newMetadata);
    }
    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
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

      return MetadataTableUtils.createMetadataTableInstance(
          ops, name(), baseTableIdentifier, identifier, type);
    } else {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }
  }

  protected boolean isValidMetadataIdentifier(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null
        && isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
  }

  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    // by default allow all identifiers
    return true;
  }

  protected Map<String, String> properties() {
    return ImmutableMap.of();
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
    private final Map<String, String> tableProperties = Maps.newHashMap();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;

    public BaseMetastoreCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      Preconditions.checkArgument(
          isValidIdentifier(identifier), "Invalid table identifier: %s", identifier);

      this.identifier = identifier;
      this.schema = schema;
      this.tableProperties.putAll(tableDefaultProperties());
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
        tableProperties.putAll(properties);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      tableProperties.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);

      try {
        ops.commit(null, metadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Table was created concurrently: %s", identifier);
      }

      return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
    }

    @Override
    public Transaction createTransaction() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
      return Transactions.createTableTransaction(
          identifier.toString(), ops, metadata, metricsReporter());
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
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }

      TableMetadata metadata;
      tableProperties.putAll(tableOverrideProperties());
      if (ops.current() != null) {
        String baseLocation = location != null ? location : ops.current().location();
        metadata =
            ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, tableProperties);
      } else {
        String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
        metadata =
            TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
      }

      if (orCreate) {
        return Transactions.createOrReplaceTableTransaction(
            identifier.toString(), ops, metadata, metricsReporter());
      } else {
        return Transactions.replaceTableTransaction(
            identifier.toString(), ops, metadata, metricsReporter());
      }
    }

    /**
     * Get default table properties set at Catalog level through catalog properties.
     *
     * @return default table properties specified in catalog properties
     */
    private Map<String, String> tableDefaultProperties() {
      Map<String, String> tableDefaultProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.TABLE_DEFAULT_PREFIX);
      LOG.info(
          "Table properties set at catalog level through catalog properties: {}",
          tableDefaultProperties);
      return tableDefaultProperties;
    }

    /**
     * Get table properties that are enforced at Catalog level through catalog properties.
     *
     * @return default table properties enforced through catalog properties
     */
    private Map<String, String> tableOverrideProperties() {
      Map<String, String> tableOverrideProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.TABLE_OVERRIDE_PREFIX);
      LOG.info(
          "Table properties enforced at catalog level through catalog properties: {}",
          tableOverrideProperties);
      return tableOverrideProperties;
    }
  }

  protected static String fullTableName(String catalogName, TableIdentifier identifier) {
    return CatalogUtil.fullTableName(catalogName, identifier);
  }

  protected MetricsReporter metricsReporter() {
    if (metricsReporter == null) {
      metricsReporter = CatalogUtil.loadMetricsReporter(properties());
    }

    return metricsReporter;
  }

  @Override
  public void close() throws IOException {
    if (metricsReporter != null) {
      metricsReporter.close();
    }
  }
}
