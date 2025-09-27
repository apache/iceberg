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
package org.apache.iceberg.flink.maintenance.operator;

import org.apache.iceberg.BaseManifestWriterFactory;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;

/** Factory to creating manifest writers. */
class ManifestWriterFactory extends BaseManifestWriterFactory {

  private final FileIO io;
  private final PartitionSpec spec;
  private final Integer rowsDivisor;

  ManifestWriterFactory(
      FileIO io,
      PartitionSpec spec,
      int formatVersion,
      String outputLocation,
      long maxManifestSizeBytes,
      Integer rowsDivisor) {
    super(formatVersion, outputLocation, maxManifestSizeBytes);
    this.io = io;
    this.spec = spec;
    this.rowsDivisor = rowsDivisor;
  }

  @Override
  public StreamingRollingManifestWriter<DataFile> newRollingManifestWriter() {
    return new StreamingRollingManifestWriter<>(
        this::newManifestWriter, maxManifestSizeBytes(), rowsDivisor);
  }

  @Override
  public StreamingRollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter() {
    return new StreamingRollingManifestWriter<>(
        this::newDeleteManifestWriter, maxManifestSizeBytes(), rowsDivisor);
  }

  @Override
  protected PartitionSpec spec() {
    return spec;
  }

  @Override
  protected FileIO fileIO() {
    return io;
  }
}
