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
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.rules.TemporaryFolder;

class LocalTableOperations implements TableOperations {
  private final TemporaryFolder temp;
  private final FileIO io;

  private final Map<String, String> createdMetadataFilePaths = Maps.newHashMap();

  LocalTableOperations(TemporaryFolder temp) {
    this.temp = temp;
    this.io = new TestTables.LocalFileIO();
  }

  @Override
  public TableMetadata current() {
    throw new UnsupportedOperationException("Not implemented for tests");
  }

  @Override
  public TableMetadata refresh() {
    throw new UnsupportedOperationException("Not implemented for tests");
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Not implemented for tests");
  }

  @Override
  public FileIO io() {
    return io;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return createdMetadataFilePaths.computeIfAbsent(
        fileName,
        name -> {
          try {
            return temp.newFile(name).getAbsolutePath();
          } catch (IOException e) {
            throw new RuntimeIOException(e);
          }
        });
  }

  @Override
  public LocationProvider locationProvider() {
    throw new UnsupportedOperationException("Not implemented for tests");
  }

  @Override
  public long newSnapshotId() {
    throw new UnsupportedOperationException("Not implemented for tests");
  }
}
