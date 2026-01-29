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
package org.apache.iceberg.index;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.IndexCatalog;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchIndexException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of {@link IndexCatalog} that provides common functionality for index catalog
 * operations.
 *
 * <p>This abstract class provides a base implementation for index catalogs, similar to how {@link
 * org.apache.iceberg.view.BaseMetastoreViewCatalog} provides a base for view catalogs.
 */
public abstract class BaseIndexCatalog implements IndexCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(BaseIndexCatalog.class);

  public static final String INDEX_DEFAULT_PREFIX = "index-default.";
  public static final String INDEX_OVERRIDE_PREFIX = "index-override.";

  private String catalogName;
  private Map<String, String> catalogProperties;

  /**
   * Create new index operations for the given identifier.
   *
   * @param identifier an index identifier
   * @return index operations for the identifier
   */
  protected abstract IndexOperations newIndexOps(IndexIdentifier identifier);

  /**
   * Return the catalog that manages tables.
   *
   * @return the table catalog
   */
  protected abstract Catalog tableCatalog();

  /**
   * Return the default location for an index.
   *
   * @param identifier the index identifier
   * @return the default location for the index
   */
  protected abstract String defaultIndexLocation(IndexIdentifier identifier);

  /**
   * Check if a table exists.
   *
   * @param identifier the table identifier
   * @return true if the table exists
   */
  protected boolean tableExists(TableIdentifier identifier) {
    return tableCatalog().tableExists(identifier);
  }

  /**
   * Check if an index identifier is valid.
   *
   * @param identifier the index identifier
   * @return true if the identifier is valid
   */
  protected boolean isValidIdentifier(IndexIdentifier identifier) {
    return identifier != null
        && identifier.tableIdentifier() != null
        && identifier.name() != null
        && !identifier.name().isEmpty();
  }

  /**
   * Return the catalog properties.
   *
   * @return the catalog properties
   */
  protected Map<String, String> properties() {
    return catalogProperties;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name;
    this.catalogProperties = properties;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Index loadIndex(IndexIdentifier identifier) {
    if (isValidIdentifier(identifier)) {
      IndexOperations ops = newIndexOps(identifier);
      if (ops.current() == null) {
        throw new NoSuchIndexException("Index does not exist: %s", identifier);
      } else {
        return new BaseIndex(newIndexOps(identifier), fullIndexName(identifier));
      }
    }

    throw new NoSuchIndexException("Invalid index identifier: %s", identifier);
  }

  @Override
  public List<IndexSummary> listIndexes(TableIdentifier tableIdentifier, IndexType... types) {
    if (!tableExists(tableIdentifier)) {
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
    }

    List<IndexSummary> allIndexes = doListIndexes(tableIdentifier);

    if (types == null || types.length == 0) {
      return allIndexes;
    }

    List<IndexType> typeFilter = Arrays.asList(types);
    return allIndexes.stream()
        .filter(summary -> typeFilter.contains(summary.type()))
        .collect(Collectors.toList());
  }

  /**
   * List all indexes for a table. Subclasses must implement this method.
   *
   * @param tableIdentifier the table identifier
   * @return a list of index summaries
   */
  protected abstract List<IndexSummary> doListIndexes(TableIdentifier tableIdentifier);

  @Override
  public IndexBuilder buildIndex(IndexIdentifier identifier) {
    return new BaseIndexBuilder(identifier);
  }

  @Override
  public boolean dropIndex(IndexIdentifier identifier) {
    if (!isValidIdentifier(identifier)) {
      return false;
    }

    IndexOperations ops = newIndexOps(identifier);
    if (ops.current() == null) {
      return false;
    }

    return doDropIndex(identifier);
  }

  /**
   * Drop an index. Subclasses must implement this method.
   *
   * @param identifier the index identifier
   * @return true if the index was dropped
   */
  protected abstract boolean doDropIndex(IndexIdentifier identifier);

  @Override
  public Index registerIndex(IndexIdentifier identifier, String metadataFileLocation) {
    Preconditions.checkArgument(
        identifier != null && isValidIdentifier(identifier), "Invalid identifier: %s", identifier);
    Preconditions.checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Cannot register an empty metadata file location as an index");

    if (indexExists(identifier)) {
      throw new AlreadyExistsException("Index already exists: %s", identifier);
    }

    if (!tableExists(identifier.tableIdentifier())) {
      throw new NoSuchTableException("Table does not exist: %s", identifier.tableIdentifier());
    }

    IndexOperations ops = newIndexOps(identifier);
    IndexMetadata metadata =
        IndexMetadataParser.read(((BaseIndexOperations) ops).io(), metadataFileLocation);
    ops.commit(null, metadata);

    return new BaseIndex(ops, fullIndexName(identifier));
  }

  /**
   * Return the full name for an index.
   *
   * @param identifier the index identifier
   * @return the full index name
   */
  protected String fullIndexName(IndexIdentifier identifier) {
    return String.format("%s.%s", name(), identifier);
  }

  /** Base implementation of {@link IndexBuilder}. */
  protected class BaseIndexBuilder implements IndexBuilder {
    private final IndexIdentifier identifier;
    private Map<String, String> properties = null;
    private Map<String, String> snapshotProperties = null;
    private Set<Long> snapshotIdsToRemove = null;
    private IndexType type = null;
    private List<Integer> indexColumnIds = null;
    private List<Integer> optimizedColumnIds = null;
    private String location = null;
    private long tableSnapshotId = -1L;
    private long indexSnapshotId = -1L;

    protected BaseIndexBuilder(IndexIdentifier identifier) {
      Preconditions.checkArgument(
          isValidIdentifier(identifier), "Invalid index identifier: %s", identifier);
      this.identifier = identifier;
    }

    /**
     * Get default index properties set at Catalog level through catalog properties.
     *
     * @return default index properties specified in catalog properties
     */
    private Map<String, String> indexDefaultProperties() {
      Map<String, String> indexDefaultProperties =
          PropertyUtil.propertiesWithPrefix(properties(), INDEX_DEFAULT_PREFIX);
      LOG.info(
          "Index properties set at catalog level through catalog properties: {}",
          indexDefaultProperties);
      return indexDefaultProperties;
    }

    /**
     * Get index properties that are enforced at Catalog level through catalog properties.
     *
     * @return overriding index properties enforced through catalog properties
     */
    private Map<String, String> indexOverrideProperties() {
      Map<String, String> indexOverrideProperties =
          PropertyUtil.propertiesWithPrefix(properties(), INDEX_OVERRIDE_PREFIX);
      LOG.info(
          "Index properties enforced at catalog level through catalog properties: {}",
          indexOverrideProperties);
      return indexOverrideProperties;
    }

    @Override
    public IndexBuilder withType(IndexType indexType) {
      this.type = indexType;
      return this;
    }

    @Override
    public IndexBuilder withIndexColumnIds(List<Integer> columnIds) {
      this.indexColumnIds = Lists.newArrayList(columnIds);
      return this;
    }

    @Override
    public IndexBuilder withIndexColumnIds(int... columnIds) {
      this.indexColumnIds = Lists.newArrayList();
      for (int columnId : columnIds) {
        this.indexColumnIds.add(columnId);
      }

      return this;
    }

    @Override
    public IndexBuilder withOptimizedColumnIds(List<Integer> columnIds) {
      this.optimizedColumnIds = Lists.newArrayList(columnIds);
      return this;
    }

    @Override
    public IndexBuilder withOptimizedColumnIds(int... columnIds) {
      this.optimizedColumnIds = Lists.newArrayList();
      for (int columnId : columnIds) {
        this.optimizedColumnIds.add(columnId);
      }

      return this;
    }

    @Override
    public IndexBuilder withProperties(Map<String, String> newProperties) {
      newProperties.forEach(this::withProperty);
      return this;
    }

    @Override
    public IndexBuilder withProperty(String key, String value) {
      if (properties == null) {
        this.properties = Maps.newHashMap();
        properties.putAll(indexDefaultProperties());
      }

      properties.put(key, value);
      return this;
    }

    @Override
    public IndexBuilder withSnapshotProperties(Map<String, String> newProperties) {
      newProperties.forEach(this::withSnapshotProperty);
      return this;
    }

    @Override
    public IndexBuilder withSnapshotProperty(String key, String value) {
      if (snapshotProperties == null) {
        this.snapshotProperties = Maps.newHashMap();
      }

      snapshotProperties.put(key, value);
      return this;
    }

    @Override
    public IndexBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public IndexBuilder withTableSnapshotId(long snapshotId) {
      this.tableSnapshotId = snapshotId;
      return this;
    }

    @Override
    public IndexBuilder withIndexSnapshotId(long snapshotId) {
      this.indexSnapshotId = snapshotId;
      return this;
    }

    @Override
    public IndexBuilder removeSnapshotById(long snapshotId) {
      if (snapshotIdsToRemove == null) {
        snapshotIdsToRemove = Sets.newHashSet();
      }

      snapshotIdsToRemove.add(snapshotId);
      return this;
    }

    @Override
    public IndexBuilder removeSnapshotsByIds(Set<Long> snapshotIds) {
      snapshotIds.forEach(this::removeSnapshotById);
      return this;
    }

    @Override
    public IndexBuilder removeSnapshotsByIds(long... snapshotIds) {
      for (long id : snapshotIds) {
        removeSnapshotById(id);
      }

      return this;
    }

    @Override
    public Index create() {
      return create(newIndexOps(identifier));
    }

    @Override
    public Index replace() {
      return replace(newIndexOps(identifier));
    }

    @Override
    public Index createOrReplace() {
      IndexOperations ops = newIndexOps(identifier);
      if (null == ops.current()) {
        return create(ops);
      } else {
        return replace(ops);
      }
    }

    private Index create(IndexOperations ops) {
      if (null != ops.current()) {
        throw new AlreadyExistsException("Index already exists: %s", identifier);
      }

      if (!tableExists(identifier.tableIdentifier())) {
        throw new NoSuchTableException("Table does not exist: %s", identifier.tableIdentifier());
      }

      Preconditions.checkState(null != type, "Cannot create index without specifying a type");
      Preconditions.checkState(
          indexColumnIds != null && !indexColumnIds.isEmpty(),
          "Cannot create index without specifying index column ids");
      Preconditions.checkState(
          optimizedColumnIds != null && !optimizedColumnIds.isEmpty(),
          "Cannot create index without specifying optimized column ids");

      IndexVersion indexVersion = indexVersion(1);
      IndexSnapshot indexSnapshot = indexSnapshot(1);
      IndexMetadata.Builder builder =
          IndexMetadata.builder()
              .setType(type)
              .setIndexColumnIds(indexColumnIds)
              .setOptimizedColumnIds(optimizedColumnIds)
              .setLocation(null != location ? location : defaultIndexLocation(identifier));

      if (indexVersion != null) {
        builder.setCurrentVersion(indexVersion);
      }

      if (indexSnapshot != null) {
        builder.addSnapshot(indexSnapshot);
      }

      if (snapshotIdsToRemove != null) {
        builder.removeSnapshots(snapshotIdsToRemove);
      }

      try {
        ops.commit(null, builder.build());
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Index was created concurrently: %s", identifier);
      }

      return new BaseIndex(ops, fullIndexName(identifier));
    }

    private Index replace(IndexOperations ops) {
      if (null == ops.current()) {
        throw new NoSuchIndexException("Index does not exist: %s", identifier);
      }

      Preconditions.checkState(type == null, "Cannot update index type");
      Preconditions.checkState(indexColumnIds == null, "Cannot update index column ids");
      Preconditions.checkState(optimizedColumnIds == null, "Cannot update optimized column ids");

      IndexMetadata metadata = ops.current();

      IndexMetadata.Builder builder = IndexMetadata.buildFrom(metadata);

      int currentVersionId = metadata.currentVersionId();
      if (properties != null) {
        int maxVersionId =
            metadata.versions().stream()
                .map(IndexVersion::versionId)
                .max(Integer::compareTo)
                .orElseGet(metadata::currentVersionId);

        builder = builder.setCurrentVersion(indexVersion(maxVersionId + 1));
        currentVersionId = maxVersionId + 1;
      }

      IndexSnapshot snapshot = indexSnapshot(currentVersionId);

      if (snapshot != null) {
        builder = builder.addSnapshot(snapshot);
      }

      if (snapshotIdsToRemove != null) {
        builder.removeSnapshots(snapshotIdsToRemove);
      }

      if (location != null) {
        builder.setLocation(location);
      }

      IndexMetadata replacement = builder.build();

      try {
        ops.commit(metadata, replacement);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Index was updated concurrently: %s", identifier);
      }

      return new BaseIndex(ops, fullIndexName(identifier));
    }

    private IndexVersion indexVersion(int versionId) {
      if (properties != null) {
        Map<String, String> mergedProperties = Maps.newHashMap(properties);
        mergedProperties.putAll(indexOverrideProperties());

        return ImmutableIndexVersion.builder()
            .versionId(versionId)
            .timestampMillis(System.currentTimeMillis())
            .properties(mergedProperties)
            .build();
      } else if (versionId == 1) {
        return ImmutableIndexVersion.builder()
            .versionId(versionId)
            .timestampMillis(System.currentTimeMillis())
            .build();
      } else {
        return null;
      }
    }

    private IndexSnapshot indexSnapshot(int versionId) {
      if (snapshotProperties != null || tableSnapshotId != -1L || indexSnapshotId != -1L) {
        Preconditions.checkArgument(
            tableSnapshotId != -1L,
            "Cannot create index snapshot without specifying tableSnapshotId");
        Preconditions.checkArgument(
            indexSnapshotId != -1L,
            "Cannot create index snapshot without specifying indexSnapshotId");

        return ImmutableIndexSnapshot.builder()
            .indexSnapshotId(indexSnapshotId)
            .tableSnapshotId(tableSnapshotId)
            .versionId(versionId)
            .properties(snapshotProperties)
            .build();
      } else {
        return null;
      }
    }
  }
}
