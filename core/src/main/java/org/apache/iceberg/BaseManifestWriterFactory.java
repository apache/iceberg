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

import java.io.Serializable;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;

/** Base class for manifest writer factories. */
public abstract class BaseManifestWriterFactory implements Serializable {
  private final int formatVersion;
  private final String outputLocation;
  private final long maxManifestSizeBytes;

  protected BaseManifestWriterFactory(
      int formatVersion, String outputLocation, long maxManifestSizeBytes) {
    this.formatVersion = formatVersion;
    this.outputLocation = outputLocation;
    this.maxManifestSizeBytes = maxManifestSizeBytes;
  }

  public RollingManifestWriter<DataFile> newRollingManifestWriter() {
    return new RollingManifestWriter<>(this::newManifestWriter, maxManifestSizeBytes);
  }

  public RollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter() {
    return new RollingManifestWriter<>(this::newDeleteManifestWriter, maxManifestSizeBytes);
  }

  protected ManifestWriter<DataFile> newManifestWriter() {
    return ManifestFiles.write(formatVersion, spec(), newOutputFile(), null);
  }

  protected ManifestWriter<DeleteFile> newDeleteManifestWriter() {
    return ManifestFiles.writeDeleteManifest(formatVersion, spec(), newOutputFile(), null);
  }

  protected OutputFile newOutputFile() {
    String fileName = FileFormat.AVRO.addExtension("optimized-m-" + UUID.randomUUID());
    return fileIO().newOutputFile(new Path(outputLocation, fileName).toString());
  }

  protected abstract PartitionSpec spec();

  protected abstract FileIO fileIO();

  public int formatVersion() {
    return formatVersion;
  }

  public long maxManifestSizeBytes() {
    return maxManifestSizeBytes;
  }
}
