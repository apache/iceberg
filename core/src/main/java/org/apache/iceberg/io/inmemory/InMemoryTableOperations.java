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

package org.apache.iceberg.io.inmemory;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

final class InMemoryTableOperations extends BaseMetastoreTableOperations {

  private final FileIO fileIO;
  private final TableIdentifier tableIdentifier;
  private final InMemoryCatalogDb catalogDb;

  InMemoryTableOperations(FileIO fileIO, TableIdentifier tableIdentifier, InMemoryCatalogDb catalogDb) {
    this.fileIO = fileIO;
    this.tableIdentifier = tableIdentifier;
    this.catalogDb = catalogDb;
  }

  @Override
  public void doRefresh() {
    String latestLocation = catalogDb.currentMetadataLocation(tableIdentifier);
    if (latestLocation == null) {
      disableRefresh();
    } else {
      refreshFromMetadataLocation(latestLocation);
    }
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    String newLocation = writeNewMetadata(metadata, currentVersion() + 1);
    String oldLocation = base == null ? null : base.metadataFileLocation();
    catalogDb.createOrUpdateTableEntry(tableIdentifier, oldLocation, newLocation);
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return tableIdentifier.toString();
  }
}
