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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

/** Responsible for producing v4 root manifest files for a commit. */
class RootManifestProducer {
  private final TableOperations ops;
  private final FileIO io;
  private final long snapshotId;
  private final Map<String, String> writerProperties = Map.of();
  private final List<TrackedFile> appendedDataFiles = Lists.newArrayList();

  // internal state
  private final String commitUUID = UUID.randomUUID().toString();
  private final AtomicInteger attempt = new AtomicInteger(0);

  // commit metadata
  private final Set<String> manifestLocations = Sets.newHashSet();
  private long assignedRowIds = 0;

  RootManifestProducer(TableOperations ops, long snapshotId) {
    this.ops = ops;
    this.io = ops.io();
    this.snapshotId = snapshotId;
  }

  void appendFile(DataFile dataFile) {
    appendedDataFiles.add(TrackedFileAdapters.asTrackedFile(dataFile, ops.current().schema()));
  }

  TrackedFile apply(TableMetadata base, Snapshot parent, long attemptSequenceNumber) {
    RootMetadata root = RootMetadata.fromSnapshot(base, parent, io);

    manifestLocations.clear();
    V4ManifestWriter rootWriter = newRootWriter(base, attemptSequenceNumber, base.nextRowId());
    try (rootWriter) {
      for (TrackedFile added : appendedDataFiles) {
        rootWriter.append(added);
      }

      if (root != null) {
        for (TrackedFile file : root.dataFiles()) {
          if (file.tracking().isLive()) {
            rootWriter.existing(file);
          }
        }

        for (TrackedFile dataManifest : root.dataManifests()) {
          if (dataManifest.tracking().isLive()) {
            rootWriter.existing(dataManifest);
            manifestLocations.add(dataManifest.location());
          }
        }

        for (TrackedFile deleteManifest : root.deleteManifests()) {
          if (deleteManifest.tracking().isLive()) {
            rootWriter.existing(deleteManifest);
            manifestLocations.add(deleteManifest.location());
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write root manifest", e);
    }

    this.assignedRowIds = rootWriter.assignedRowIds();

    return rootWriter.toTrackedManifest();
  }

  long assignedRowIds() {
    return assignedRowIds;
  }

  Set<String> manifestLocations() {
    return ImmutableSet.copyOf(manifestLocations);
  }

  private EncryptedOutputFile newRootOutputFile() {
    String filename =
        String.format(
            Locale.ROOT, "root-%d-%d-%s", snapshotId, attempt.incrementAndGet(), commitUUID);
    String rootManifestLocation =
        ops.metadataFileLocation(FileFormat.PARQUET.addExtension(filename));
    OutputFile rawOutputFile = ops.io().newOutputFile(rootManifestLocation);
    return ops.encryption().encrypt(rawOutputFile);
  }

  private V4ManifestWriter newRootWriter(TableMetadata base, long sequenceNumber, long firstRowId) {
    // always use the latest stats type to prevent dropping stats from new files in the root
    Types.StructType statsType =
        StatsUtil.statsWriteSchema(base.schema(), MetricsConfig.forTable(base));
    return V4ManifestWriter.createRoot(
        newRootOutputFile(),
        base.specsById(),
        statsType,
        writerProperties,
        base.currentSchemaId(),
        snapshotId,
        sequenceNumber,
        firstRowId);
  }

  private record RootMetadata(
      List<TrackedFile> dataFiles,
      List<TrackedFile> dataManifests,
      List<TrackedFile> deleteManifests) {
    private static RootMetadata fromRootManifest(TableMetadata meta, Snapshot snapshot, FileIO io) {
      ManifestFile root = RootManifestFile.from(snapshot, meta.location(), io);

      ImmutableList.Builder<TrackedFile> rootDataFiles = ImmutableList.builder();
      ImmutableList.Builder<TrackedFile> dataManifests = ImmutableList.builder();
      ImmutableList.Builder<TrackedFile> deleteManifests = ImmutableList.builder();

      try (V4ManifestReader reader =
          V4ManifestReader.builder(root, io, meta.schema(), meta.specsById())
              .tableLocation(meta.location())
              .metricsConfig(MetricsConfig.forTable(meta))
              .build()) {

        for (TrackedFile file : reader) {
          switch (file.contentType()) {
            case DATA -> rootDataFiles.add(file);
            case DATA_MANIFEST -> dataManifests.add(file);
            case DELETE_MANIFEST -> deleteManifests.add(file);
            default ->
                throw new IllegalStateException(
                    String.format(
                        "Invalid %s file in root manifest: %s",
                        file.contentType(), file.location()));
          }
        }

        return new RootMetadata(
            rootDataFiles.build(), dataManifests.build(), deleteManifests.build());
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read root manifest: " + root.path(), e);
      }
    }

    private static RootMetadata fromSnapshot(TableMetadata meta, Snapshot snapshot, FileIO io) {
      if (snapshot == null) {
        return null;
      } else if (snapshot.manifestListLocation() != null) {
        List<TrackedFile> dataManifests =
            Lists.transform(snapshot.dataManifests(io), TrackedFileAdapters::asTrackedFile);
        List<TrackedFile> deleteManifests =
            Lists.transform(snapshot.deleteManifests(io), TrackedFileAdapters::asTrackedFile);
        return new RootMetadata(ImmutableList.of(), dataManifests, deleteManifests);
      }

      return fromRootManifest(meta, snapshot, io);
    }
  }
}
