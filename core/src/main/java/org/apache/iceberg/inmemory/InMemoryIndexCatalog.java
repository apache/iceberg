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
package org.apache.iceberg.inmemory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchIndexException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.index.BaseIndexCatalog;
import org.apache.iceberg.index.BaseIndexOperations;
import org.apache.iceberg.index.ImmutableIndexSummary;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexOperations;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.IndexSummary;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Index catalog implementation that uses in-memory data-structures to store the indexes. This class
 * doesn't touch external resources and can be utilized to write unit tests without side effects. It
 * uses {@link InMemoryFileIO}.
 */
public class InMemoryIndexCatalog extends BaseIndexCatalog implements Closeable {
  private static final Joiner SLASH = Joiner.on("/");

  private final ConcurrentMap<IndexIdentifier, String> indexes;
  private final Catalog tableCatalog;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;
  private CloseableGroup closeableGroup;
  private Map<String, String> catalogProperties;

  public InMemoryIndexCatalog(Catalog tableCatalog) {
    Preconditions.checkArgument(tableCatalog != null, "Table catalog cannot be null");
    this.indexes = Maps.newConcurrentMap();
    this.tableCatalog = tableCatalog;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    this.catalogName = name != null ? name : InMemoryIndexCatalog.class.getSimpleName();
    this.catalogProperties = ImmutableMap.copyOf(properties);

    String warehouse = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    this.warehouseLocation = warehouse.replaceAll("/*$", "");
    this.io = new InMemoryFileIO();
    this.closeableGroup = new CloseableGroup();
    closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  protected Catalog tableCatalog() {
    return tableCatalog;
  }

  @Override
  protected IndexOperations newIndexOps(IndexIdentifier identifier) {
    return new InMemoryIndexOperations(io, identifier);
  }

  @Override
  protected String defaultIndexLocation(IndexIdentifier identifier) {
    return SLASH.join(
        warehouseLocation,
        SLASH.join(identifier.tableIdentifier().namespace().levels()),
        identifier.tableIdentifier().name(),
        "indexes",
        identifier.name());
  }

  @Override
  protected List<IndexSummary> doListIndexes(TableIdentifier tableIdentifier) {
    return indexes.keySet().stream()
        .filter(idx -> idx.tableIdentifier().equals(tableIdentifier))
        .sorted(Comparator.comparing(IndexIdentifier::toString))
        .map(this::loadIndexSummary)
        .collect(Collectors.toList());
  }

  private IndexSummary loadIndexSummary(IndexIdentifier identifier) {
    IndexOperations ops = newIndexOps(identifier);
    IndexMetadata metadata = ops.current();
    if (metadata == null) {
      throw new NoSuchIndexException("Index does not exist: %s", identifier);
    }

    long[] availableSnapshots =
        metadata.snapshots().stream().mapToLong(IndexSnapshot::tableSnapshotId).toArray();

    return ImmutableIndexSummary.builder()
        .id(identifier)
        .type(metadata.type())
        .indexColumnIds(metadata.indexColumnIds().stream().mapToInt(Integer::intValue).toArray())
        .optimizedColumnIds(
            metadata.optimizedColumnIds().stream().mapToInt(Integer::intValue).toArray())
        .availableTableSnapshots(availableSnapshots)
        .build();
  }

  @Override
  protected boolean doDropIndex(IndexIdentifier identifier) {
    synchronized (this) {
      return null != indexes.remove(identifier);
    }
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public void close() throws IOException {
    closeableGroup.close();
    indexes.clear();
  }

  private class InMemoryIndexOperations extends BaseIndexOperations {
    private final FileIO fileIO;
    private final IndexIdentifier indexIdentifier;
    private final String fullIndexName;

    InMemoryIndexOperations(FileIO fileIO, IndexIdentifier indexIdentifier) {
      this.fileIO = fileIO;
      this.indexIdentifier = indexIdentifier;
      this.fullIndexName = fullIndexName(indexIdentifier);
    }

    @Override
    public void doRefresh() {
      String latestLocation = indexes.get(indexIdentifier);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(latestLocation);
      }
    }

    @Override
    public void doCommit(IndexMetadata base, IndexMetadata metadata) {
      String newLocation = writeNewMetadataIfRequired(metadata);
      String oldLocation = base == null ? null : currentMetadataLocation();

      synchronized (InMemoryIndexCatalog.this) {
        if (null == base && !tableExists(indexIdentifier.tableIdentifier())) {
          throw new NoSuchTableException(
              "Cannot create index %s. Table does not exist: %s",
              indexIdentifier, indexIdentifier.tableIdentifier());
        }

        indexes.compute(
            indexIdentifier,
            (k, existingLocation) -> {
              if (!Objects.equal(existingLocation, oldLocation)) {
                if (null == base) {
                  throw new AlreadyExistsException("Index already exists: %s", indexName());
                }

                if (null == existingLocation) {
                  throw new NoSuchIndexException("Index does not exist: %s", indexName());
                }

                throw new CommitFailedException(
                    "Cannot commit to index %s metadata location from %s to %s "
                        + "because it has been concurrently modified to %s",
                    indexIdentifier, oldLocation, newLocation, existingLocation);
              }

              return newLocation;
            });
      }
    }

    @Override
    protected FileIO io() {
      return fileIO;
    }

    @Override
    protected String indexName() {
      return fullIndexName;
    }
  }
}
