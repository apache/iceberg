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
 *
 */

package org.apache.iceberg;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * TableOperations implementation that provides access to metadata for a Table at some point in time, using a
 * table metadata location. It will never refer to a different Metadata object than the one it was created with
 * and cannot be used to create or delete files.
 */
public class StaticTableOperations implements TableOperations {
  private final TableMetadata staticMetadata;
  private final FileIO io;

  /**
   * Creates a StaticTableOperations tied to a specific static version of the TableMetadata
   */
  public StaticTableOperations(String metadataFileLocation, FileIO io) {
    this.io = io;
    this.staticMetadata = TableMetadataParser.read(io, metadataFileLocation);
  }

  @Override
  public TableMetadata current() {
    return staticMetadata;
  }

  @Override
  public TableMetadata refresh() {
    return staticMetadata;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Cannot modify a static table");
  }

  @Override
  public FileIO io() {
    return this.io;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    throw new UnsupportedOperationException("Cannot modify a static table");
  }

  @Override
  public LocationProvider locationProvider() {
    throw new UnsupportedOperationException("Cannot modify a static table");
  }
}
