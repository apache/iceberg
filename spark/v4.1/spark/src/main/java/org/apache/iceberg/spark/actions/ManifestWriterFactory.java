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
package org.apache.iceberg.spark.actions;

import java.io.Serializable;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RollingManifestWriter;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.spark.broadcast.Broadcast;

/** Creates the writers that Spark actions use on executors to write new manifests. */
class ManifestWriterFactory implements Serializable {
  private final Broadcast<Table> tableBroadcast;
  private final int formatVersion;
  private final int specId;
  private final String outputLocation;
  private final String manifestPrefix;
  private final long maxManifestSizeBytes;

  ManifestWriterFactory(
      Broadcast<Table> tableBroadcast,
      int formatVersion,
      int specId,
      String outputLocation,
      String manifestPrefix,
      long maxManifestSizeBytes) {
    this.tableBroadcast = tableBroadcast;
    this.formatVersion = formatVersion;
    this.specId = specId;
    this.outputLocation = outputLocation;
    this.manifestPrefix = manifestPrefix;
    this.maxManifestSizeBytes = maxManifestSizeBytes;
  }

  /** Returns the location of the table's metadata files, where new manifests go by default. */
  static String defaultOutputLocation(Table table) {
    TableOperations ops = ((HasTableOperations) table).operations();
    Path metadataFilePath = new Path(ops.metadataFileLocation("file"));
    return metadataFilePath.getParent().toString();
  }

  RollingManifestWriter<DataFile> newRollingManifestWriter() {
    return new RollingManifestWriter<>(this::newManifestWriter, maxManifestSizeBytes);
  }

  private ManifestWriter<DataFile> newManifestWriter() {
    return ManifestFiles.write(formatVersion, spec(), newOutputFile(), null);
  }

  RollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter() {
    return new RollingManifestWriter<>(this::newDeleteManifestWriter, maxManifestSizeBytes);
  }

  private ManifestWriter<DeleteFile> newDeleteManifestWriter() {
    return ManifestFiles.writeDeleteManifest(formatVersion, spec(), newOutputFile(), null);
  }

  private PartitionSpec spec() {
    return table().specs().get(specId);
  }

  private EncryptedOutputFile newOutputFile() {
    Table table = table();
    OutputFile rawOutputFile = table.io().newOutputFile(newManifestLocation());
    return table.encryption().encrypt(rawOutputFile);
  }

  private String newManifestLocation() {
    String fileName = FileFormat.AVRO.addExtension(manifestPrefix + UUID.randomUUID());
    Path filePath = new Path(outputLocation, fileName);
    return filePath.toString();
  }

  private Table table() {
    return tableBroadcast.value();
  }
}
