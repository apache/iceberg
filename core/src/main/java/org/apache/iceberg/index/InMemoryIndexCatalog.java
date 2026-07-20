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

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * In-memory implementation of {@link IndexCatalog}.
 *
 * <p>Stores index metadata in a thread-safe map. Suitable for unit tests and local development.
 * Does not persist state across JVM restarts.
 *
 * <p>Optimistic concurrency: {@link #updateIndex} checks that the base metadata file location
 * matches what is currently stored before replacing it.
 */
public class InMemoryIndexCatalog implements IndexCatalog {

  // Maps IndexIdentifier → current IndexMetadata
  private final Map<IndexIdentifier, IndexMetadata> store = new ConcurrentHashMap<>();

  @Override
  public void createIndex(IndexIdentifier identifier, IndexMetadata metadata) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    Preconditions.checkNotNull(metadata, "metadata is required");
    Preconditions.checkArgument(
        metadata.metadataFileLocation() != null,
        "metadata must have a metadataFileLocation set before registering");

    IndexMetadata existing = store.putIfAbsent(identifier, metadata);
    if (existing != null) {
      throw new AlreadyExistsException("Index already exists: %s", identifier);
    }
  }

  @Override
  public IndexMetadata loadIndex(IndexIdentifier identifier) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    IndexMetadata metadata = store.get(identifier);
    if (metadata == null) {
      throw new NoSuchTableException("Index does not exist: %s", identifier);
    }
    return metadata;
  }

  @Override
  public void updateIndex(
      IndexIdentifier identifier, IndexMetadata base, IndexMetadata updated) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    Preconditions.checkNotNull(base, "base metadata is required");
    Preconditions.checkNotNull(updated, "updated metadata is required");
    Preconditions.checkArgument(
        updated.metadataFileLocation() != null,
        "updated metadata must have a metadataFileLocation set");

    // Optimistic concurrency: only update if the current location matches base
    boolean replaced =
        store.compute(
            identifier,
            (key, current) -> {
              if (current == null) {
                throw new NoSuchTableException("Index does not exist: %s", identifier);
              }
              if (!Objects.equals(
                  current.metadataFileLocation(), base.metadataFileLocation())) {
                return null; // signal conflict
              }
              return updated;
            })
            != null;

    if (!replaced) {
      throw new ConcurrentModificationException(
          String.format(
              "Cannot update index %s: current metadata location has changed. "
                  + "Expected: %s",
              identifier, base.metadataFileLocation()));
    }
  }

  @Override
  public void dropIndex(IndexIdentifier identifier) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    IndexMetadata removed = store.remove(identifier);
    if (removed == null) {
      throw new NoSuchTableException("Index does not exist: %s", identifier);
    }
  }

  @Override
  public boolean indexExists(IndexIdentifier identifier) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    return store.containsKey(identifier);
  }

  @Override
  public List<IndexMetadata> listIndexes(TableIdentifier tableIdentifier) {
    Preconditions.checkNotNull(tableIdentifier, "tableIdentifier is required");
    return store.entrySet().stream()
        .filter(e -> e.getKey().tableIdentifier().equals(tableIdentifier))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }
}
