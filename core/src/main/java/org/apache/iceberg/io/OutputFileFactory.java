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

package org.apache.iceberg.io;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;

/**
 * Factory responsible for generating unique but recognizable data file names.
 */
public class OutputFileFactory {
  private final PartitionSpec defaultSpec;
  private final FileFormat format;
  private final LocationProvider locations;
  private final FileIO io;
  private final EncryptionManager encryptionManager;
  private final int partitionId;
  private final long taskId;
  // The purpose of this uuid is to be able to know from two paths that they were written by the same operation.
  // That's useful, for example, if a Spark job dies and leaves files in the file system, you can identify them all
  // with a recursive listing and grep.
  private final String operationId;
  private final AtomicInteger fileCount = new AtomicInteger(0);

  @Deprecated
  public OutputFileFactory(Table table, FileFormat format, int partitionId, long taskId) {
    this(table.spec(), format, table.locationProvider(), table.io(), table.encryption(),
        partitionId, taskId, UUID.randomUUID().toString());
  }

  @Deprecated
  public OutputFileFactory(Table table, PartitionSpec spec, FileFormat format, int partitionId, long taskId) {
    this(spec, format, table.locationProvider(), table.io(), table.encryption(),
        partitionId, taskId, UUID.randomUUID().toString());
  }

  /**
   * Constructor where a generated UUID is used as the operationId to ensure uniqueness.
   * @param spec Partition specification used by the location provider
   * @param format File format used for the extension
   * @param locations Location provider used for generating locations
   * @param io FileIO to store the files
   * @param encryptionManager Encryption manager used for encrypting the files
   * @param partitionId First part of the file name
   * @param taskId Second part of the file name
   * @deprecated since 0.12.0, will be removed in 0.13.0; use {@link #builderFor(Table, FileFormat, int, long)} instead.
   */
  @Deprecated
  public OutputFileFactory(PartitionSpec spec, FileFormat format, LocationProvider locations, FileIO io,
                           EncryptionManager encryptionManager, int partitionId, long taskId) {
    this(spec, format, locations, io, encryptionManager, partitionId, taskId, UUID.randomUUID().toString());
  }

  /**
   * Constructor with specific operationId. The [partitionId, taskId, operationId] triplet has to be unique across JVM
   * instances otherwise the same file name could be generated by different instances of the OutputFileFactory.
   * @param spec Partition specification used by the location provider
   * @param format File format used for the extension
   * @param locations Location provider used for generating locations
   * @param io FileIO to store the files
   * @param encryptionManager Encryption manager used for encrypting the files
   * @param partitionId First part of the file name
   * @param taskId Second part of the file name
   * @param operationId Third part of the file name
   * @deprecated since 0.12.0, will be removed in 0.13.0; use {@link #builderFor(Table, FileFormat, int, long)} instead.
   */
  @Deprecated
  public OutputFileFactory(PartitionSpec spec, FileFormat format, LocationProvider locations, FileIO io,
                           EncryptionManager encryptionManager, int partitionId, long taskId, String operationId) {
    this.defaultSpec = spec;
    this.format = format;
    this.locations = locations;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.operationId = operationId;
  }

  public static Builder builderFor(Table table, FileFormat format, int partitionId, long taskId) {
    return new Builder(table, format, partitionId, taskId);
  }

  private String generateFilename() {
    return format.addExtension(
        String.format("%05d-%d-%s-%05d", partitionId, taskId, operationId, fileCount.incrementAndGet()));
  }

  /**
   * Generates an {@link EncryptedOutputFile} for unpartitioned writes.
   */
  public EncryptedOutputFile newOutputFile() {
    OutputFile file = io.newOutputFile(locations.newDataLocation(generateFilename()));
    return encryptionManager.encrypt(file);
  }

  /**
   * Generates an {@link EncryptedOutputFile} for partitioned writes in the default spec.
   */
  public EncryptedOutputFile newOutputFile(StructLike partition) {
    return newOutputFile(defaultSpec, partition);
  }

  /**
   * Generates an {@link EncryptedOutputFile} for partitioned writes in a given spec.
   */
  public EncryptedOutputFile newOutputFile(PartitionSpec spec, StructLike partition) {
    String newDataLocation = locations.newDataLocation(spec, partition, generateFilename());
    OutputFile rawOutputFile = io.newOutputFile(newDataLocation);
    return encryptionManager.encrypt(rawOutputFile);
  }

  public static class Builder {
    private final Table table;
    private final FileFormat format;
    private final int partitionId;
    private final long taskId;
    private PartitionSpec defaultSpec;
    private String operationId;

    private Builder(Table table, FileFormat format, int partitionId, long taskId) {
      this.table = table;
      this.format = format;
      this.partitionId = partitionId;
      this.taskId = taskId;
      this.defaultSpec = table.spec();
      this.operationId = UUID.randomUUID().toString();
    }

    public Builder defaultSpec(PartitionSpec newDefaultSpec) {
      this.defaultSpec = newDefaultSpec;
      return this;
    }

    public Builder operationId(String newOperationId) {
      this.operationId = newOperationId;
      return this;
    }

    public OutputFileFactory build() {
      LocationProvider locations = table.locationProvider();
      FileIO io = table.io();
      EncryptionManager encryption = table.encryption();
      return new OutputFileFactory(defaultSpec, format, locations, io, encryption, partitionId, taskId, operationId);
    }
  }
}
