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

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;

import java.io.IOException;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.apache.iceberg.util.ThreadPools;

class ManifestWriteManager {
  static final int MIN_FILE_GROUP_SIZE = 10_000;

  private final int formatVersion;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final Function<String, String> metadataLocationProvider;
  private final Map<String, String> writerProperties;
  private final long targetManifestSizeBytes;
  private final long snapshotId;
  private final String commitUUID = UUID.randomUUID().toString();
  private final AtomicInteger manifestCount = new AtomicInteger(0);
  private ExecutorService writePool = ThreadPools.getWorkerPool();
  private int writePoolParallelism = ThreadPools.WORKER_THREAD_POOL_SIZE;

  public ManifestWriteManager(TableOperations ops, long snapshotId) {
    this.formatVersion = ops.current().formatVersion();
    this.io = ops.io();
    this.encryption = ops.encryption();
    this.metadataLocationProvider = ops::metadataFileLocation;
    this.writerProperties = manifestWriterProperties(ops.current());
    this.targetManifestSizeBytes =
        ops.current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    this.snapshotId = snapshotId;
  }

  /**
   * Configure the worker pool and target parallelism.
   *
   * @param pool an {@link ExecutorService} thread pool
   * @param parallelism target number of parallel write tasks
   */
  void writeWith(ExecutorService pool, int parallelism) {
    Preconditions.checkArgument(pool != null, "Executor service cannot be null");
    Preconditions.checkArgument(
        parallelism > 0, "Parallelism must be greater than 0, but was: %s", parallelism);
    this.writePool = pool;
    this.writePoolParallelism = parallelism;
  }

  /** Returns an {@link EncryptedOutputFile} for a new v1-v3 manifest. */
  EncryptedOutputFile newManifestOutputFile() {
    String manifestFileLocation =
        metadataLocationProvider.apply(
            FileFormat.AVRO.addExtension(commitUUID + "-m" + manifestCount.getAndIncrement()));
    OutputFile rawOutputFile = io.newOutputFile(manifestFileLocation);
    return encryption.encrypt(rawOutputFile);
  }

  /** Returns a {@link ManifestWriter} for data files. */
  ManifestWriter<DataFile> newManifestWriter(PartitionSpec spec) {
    return ManifestFiles.write(
        formatVersion, spec, newManifestOutputFile(), snapshotId, writerProperties);
  }

  /**
   * Writes a collection of data files to one or more new data manifests using a rolling writer.
   *
   * <p>All data files be partitioned by the same provided spec.
   *
   * <p>When the number of files is over the parallelism threshold, this will write multiple files
   * in parallel.
   *
   * @param files a collection of {@link DataFile} to write
   * @param spec the partition spec of all data files
   * @return {@link ManifestFile} metadata for all newly written manifest files
   */
  List<ManifestFile> writeDataManifests(Collection<DataFile> files, PartitionSpec spec) {
    int groupCount = manifestWriterCount(writePoolParallelism, files.size());
    return ManifestFiles.writeParallel(
        files, groupCount, writePool, group -> writeDataFileGroup(group, spec));
  }

  /** Returns a {@link ManifestWriter} for delete files. */
  ManifestWriter<DeleteFile> newDeleteManifestWriter(PartitionSpec spec) {
    return ManifestFiles.writeDeleteManifest(
        formatVersion, spec, newManifestOutputFile(), snapshotId, writerProperties);
  }

  /**
   * Writes a collection of delete files to one or more new delete manifests using a rolling writer.
   *
   * <p>All delete files be partitioned by the same provided spec.
   *
   * <p>When the number of files is over the parallelism threshold, this will write multiple files
   * in parallel.
   *
   * @param files a collection of {@link DeleteFile} to write
   * @param spec the partition spec of all delete files
   * @return {@link ManifestFile} metadata for all newly written manifest files
   */
  List<ManifestFile> writeDeleteManifests(Collection<DeleteFile> files, PartitionSpec spec) {
    int groupCount = manifestWriterCount(writePoolParallelism, files.size());
    return ManifestFiles.writeParallel(
        files, groupCount, writePool, group -> writeDeleteFileGroup(group, spec));
  }

  private List<ManifestFile> writeDataFileGroup(Collection<DataFile> files, PartitionSpec spec) {
    RollingManifestWriter<DataFile> writer = newRollingManifestWriter(spec);

    try (RollingManifestWriter<DataFile> closeableWriter = writer) {
      for (DataFile file : files) {
        // data sequence number is replaced when files are added
        if (file.dataSequenceNumber() != null) {
          closeableWriter.add(file, file.dataSequenceNumber());
        } else {
          closeableWriter.add(file);
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write data manifests");
    }

    return writer.toManifestFiles();
  }

  private RollingManifestWriter<DataFile> newRollingManifestWriter(PartitionSpec spec) {
    return new RollingManifestWriter<>(() -> newManifestWriter(spec), targetManifestSizeBytes);
  }

  private List<ManifestFile> writeDeleteFileGroup(
      Collection<DeleteFile> files, PartitionSpec spec) {
    RollingManifestWriter<DeleteFile> writer = newRollingDeleteManifestWriter(spec);

    try (RollingManifestWriter<DeleteFile> closableWriter = writer) {
      for (DeleteFile file : files) {
        if (file.dataSequenceNumber() != null) {
          closableWriter.add(file, file.dataSequenceNumber());
        } else {
          closableWriter.add(file);
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write delete manifests");
    }

    return writer.toManifestFiles();
  }

  private RollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter(PartitionSpec spec) {
    return new RollingManifestWriter<>(
        () -> newDeleteManifestWriter(spec), targetManifestSizeBytes);
  }

  /**
   * Calculates how many manifest writers can be used concurrently to handle the given number of
   * files without creating too small manifests.
   *
   * @param workerPoolSize the size of the available worker pool
   * @param fileCount the total number of files to be processed
   * @return the number of manifest writers that can be used concurrently
   */
  @VisibleForTesting
  static int manifestWriterCount(int workerPoolSize, int fileCount) {
    int limit = IntMath.divide(fileCount, MIN_FILE_GROUP_SIZE, RoundingMode.HALF_UP);
    return Math.max(1, Math.min(workerPoolSize, limit));
  }

  private static Map<String, String> manifestWriterProperties(TableMetadata metadata) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    String codec =
        metadata.property(
            TableProperties.MANIFEST_COMPRESSION, TableProperties.MANIFEST_COMPRESSION_DEFAULT);
    builder.put(TableProperties.AVRO_COMPRESSION, codec);

    String level =
        metadata.property(
            TableProperties.MANIFEST_COMPRESSION_LEVEL,
            TableProperties.MANIFEST_COMPRESSION_LEVEL_DEFAULT);
    if (level != null) {
      builder.put(TableProperties.AVRO_COMPRESSION_LEVEL, level);
    }

    return builder.build();
  }
}
