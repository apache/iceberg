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
package org.apache.iceberg.view;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.DummyFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

public class ViewOperationWrapper implements TableOperations {

  private final ViewOperations viewOperations;

  public ViewOperationWrapper(ViewOperations viewOperations) {
    this.viewOperations = viewOperations;
  }

  @Override
  public TableMetadata current() {
    return wrappingViewMetadata(viewOperations.current());
  }

  @Override
  public TableMetadata refresh() {
    return wrappingViewMetadata(viewOperations.refresh());
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Unable to commit WrappedViewOperation");
  }

  @Override
  public FileIO io() {
    return new DummyFileIO();
  }

  @Override
  public EncryptionManager encryption() {
    return TableOperations.super.encryption();
  }

  @Override
  public String metadataFileLocation(String fileName) {
    throw new UnsupportedOperationException("Unable to commit WrappedViewOperation");
  }

  @Override
  public LocationProvider locationProvider() {
    throw new UnsupportedOperationException("Unable to commit WrappedViewOperation");
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    throw new UnsupportedOperationException("Unable to commit WrappedViewOperation");
  }

  @Override
  public long newSnapshotId() {
    throw new UnsupportedOperationException("Unable to commit WrappedViewOperation");
  }

  @Override
  public boolean requireStrictCleanup() {
    return false;
  }

  private TableMetadata wrappingViewMetadata(ViewMetadata viewMetadata) {
    TableMetadata.Builder builder = TableMetadata.buildFromEmpty();
    viewMetadata.schemas().forEach(builder::addSchema);
    return builder
        .setCurrentSchema(viewMetadata.currentSchemaId())
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .setLocation(viewMetadata.location())
        .setProperties(viewMetadata.properties())
        .assignUUID(viewMetadata.uuid())
        .discardChanges()
        .withMetadataLocation(viewMetadata.metadataFileLocation())
        .build();
  }
}
