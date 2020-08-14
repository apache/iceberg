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

import java.nio.file.Paths;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * Representation of an immutable snapshot of Table State that can be used in
 * other Iceberg Functions.
 */
public class StaticTableOperations implements TableOperations {
  private final TableMetadata staticMetadata;
  private final FileIO io;

  /**
   * Creates a StaticTableOperations tied to a specific static version of the TableMetadata
   */
  public StaticTableOperations(String location, FileIO io) {
    this.io = io;
    this.staticMetadata = TableMetadataParser.read(io, location);
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
    throw new UnsupportedOperationException("This TableOperations is static, it cannot be modified");
  }

  @Override
  public FileIO io() {
    return this.io;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    throw new UnsupportedOperationException("New files cannot be created in a Static Table Operations");
  }

  @Override
  public LocationProvider locationProvider() {
    throw new UnsupportedOperationException("New files cannot be created in a Static Table Operations");
  }
}
