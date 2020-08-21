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
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;

public class OutputFileFactory {
  private final PartitionSpec spec;
  private final FileFormat format;
  private final LocationProvider locations;
  private final FileIO io;
  private final EncryptionManager encryptionManager;
  private final int partitionId;
  private final long taskId;
  // The purpose of this uuid is to be able to know from two paths that they were written by the same operation.
  // That's useful, for example, if a Spark job dies and leaves files in the file system, you can identify them all
  // with a recursive listing and grep.
  private final String uuid = UUID.randomUUID().toString();
  private final AtomicInteger fileCount = new AtomicInteger(0);

  public OutputFileFactory(PartitionSpec spec, FileFormat format, LocationProvider locations, FileIO io,
                           EncryptionManager encryptionManager, int partitionId, long taskId) {
    this.spec = spec;
    this.format = format;
    this.locations = locations;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.partitionId = partitionId;
    this.taskId = taskId;
  }

  private String generateFilename() {
    return format.addExtension(
        String.format("%05d-%d-%s-%05d", partitionId, taskId, uuid, fileCount.incrementAndGet()));
  }

  /**
   * Generates EncryptedOutputFile for UnpartitionedWriter.
   */
  public EncryptedOutputFile newOutputFile() {
    OutputFile file = io.newOutputFile(locations.newDataLocation(generateFilename()));
    return encryptionManager.encrypt(file);
  }

  /**
   * Generates EncryptedOutputFile for PartitionedWriter.
   */
  public EncryptedOutputFile newOutputFile(PartitionKey key) {
    String newDataLocation = locations.newDataLocation(spec, key, generateFilename());
    OutputFile rawOutputFile = io.newOutputFile(newDataLocation);
    return encryptionManager.encrypt(rawOutputFile);
  }
}
