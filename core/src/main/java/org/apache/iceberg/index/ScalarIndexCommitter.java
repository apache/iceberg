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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Commits a completed index build to the {@link IndexCatalog}.
 *
 * <p>Given a list of {@link LeafFileMetadata} produced by the Spark build job, this class:
 * <ol>
 *   <li>Writes the tracking file (Avro) to the index metadata directory.
 *   <li>Creates a new {@link IndexSnapshot} pointing to the tracking file.
 *   <li>Writes the new index metadata JSON file.
 *   <li>Atomically registers or updates the index in the {@link IndexCatalog}.
 * </ol>
 *
 * <p>The commit is optimistic: if another writer has already committed a newer version,
 * {@link java.util.ConcurrentModificationException} is thrown and the caller should retry.
 */
public class ScalarIndexCommitter {

  private final IndexCatalog catalog;
  private final FileIO fileIO;

  public ScalarIndexCommitter(IndexCatalog catalog, FileIO fileIO) {
    Preconditions.checkNotNull(catalog, "IndexCatalog is required");
    Preconditions.checkNotNull(fileIO, "FileIO is required");
    this.catalog = catalog;
    this.fileIO = fileIO;
  }

  /**
   * Commit a full index build or incremental update.
   *
   * @param identifier identifies the index in the catalog
   * @param tableUuid UUID of the source Iceberg table
   * @param sourceTableSnapshotId the table snapshot the index was built from
   * @param indexType e.g. {@code "SCALAR"}
   * @param transformFunction e.g. {@code "HASH"} or {@code "IDENTITY"}
   * @param keyColumnIds source-table column IDs used as index keys
   * @param includedColumnIds optional covering column IDs (may be empty)
   * @param properties additional index properties (e.g. hash.num-buckets)
   * @param indexLocation base location for index files
   * @param leafFiles metadata of the leaf files written by the build job
   */
  public void commit(
      IndexIdentifier identifier,
      String tableUuid,
      long sourceTableSnapshotId,
      String indexType,
      String transformFunction,
      List<Integer> keyColumnIds,
      List<Integer> includedColumnIds,
      Map<String, String> properties,
      String indexLocation,
      List<LeafFileMetadata> leafFiles) {

    Preconditions.checkArgument(
        leafFiles != null && !leafFiles.isEmpty(), "leafFiles must be non-empty");

    // Load current metadata if the index already exists (incremental update)
    IndexMetadata current = null;
    if (catalog.indexExists(identifier)) {
      current = catalog.loadIndex(identifier);
    }

    // 1. Write tracking file (Avro)
    String trackingFilePath = newTrackingFilePath(indexLocation, current);
    OutputFile trackingOutput = fileIO.newOutputFile(trackingFilePath);
    try (TrackingFileWriter writer = new TrackingFileWriter(trackingOutput)) {
      for (LeafFileMetadata lf : leafFiles) {
        writer.add(lf.toTrackingEntry());
      }
    }

    // 2. Build new IndexSnapshot
    long newSnapshotId = Math.abs(ThreadLocalRandom.current().nextLong());
    IndexSnapshot snapshot =
        GenericIndexSnapshot.builder()
            .snapshotId(newSnapshotId)
            .sourceTableSnapshotId(sourceTableSnapshotId)
            .timestampMs(System.currentTimeMillis())
            .trackingFile(trackingFilePath)
            .build();

    // 3. Build updated IndexMetadata
    GenericIndexMetadata.Builder metaBuilder;
    if (current == null) {
      metaBuilder =
          GenericIndexMetadata.builder()
              .uuid(UUID.randomUUID().toString())
              .tableUuid(tableUuid)
              .location(indexLocation)
              .type(indexType)
              .transformFunction(transformFunction)
              .keyColumnIds(keyColumnIds)
              .includedColumnIds(includedColumnIds != null ? includedColumnIds : ImmutableList.of())
              .properties(properties != null ? properties : ImmutableMap.of());
    } else {
      metaBuilder = GenericIndexMetadata.buildFrom(current);
    }
    metaBuilder.addSnapshot(snapshot);

    // 4. Write new metadata file
    String newMetadataPath = IndexMetadataIO.newMetadataFileLocation(indexLocation, current);
    IndexMetadata updated = metaBuilder.metadataFileLocation(newMetadataPath).build();
    IndexMetadataIO.write(updated, fileIO.newOutputFile(newMetadataPath));

    // 5. Register or update in catalog (optimistic CAS)
    if (current == null) {
      catalog.createIndex(identifier, updated);
    } else {
      catalog.updateIndex(identifier, current, updated);
    }
  }

  /** Convenience overload without included columns or extra properties. */
  public void commit(
      IndexIdentifier identifier,
      String tableUuid,
      long sourceTableSnapshotId,
      String indexType,
      String transformFunction,
      List<Integer> keyColumnIds,
      String indexLocation,
      List<LeafFileMetadata> leafFiles) {
    commit(
        identifier,
        tableUuid,
        sourceTableSnapshotId,
        indexType,
        transformFunction,
        keyColumnIds,
        ImmutableList.of(),
        ImmutableMap.of(),
        indexLocation,
        leafFiles);
  }

  private static String newTrackingFilePath(String indexLocation, IndexMetadata current) {
    int version = current == null ? 1 : current.snapshots().size() + 1;
    String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    return String.format(
        "%s/metadata/tracking-%05d-%s.avro",
        indexLocation.replaceAll("/$", ""), version, suffix);
  }
}
