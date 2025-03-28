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

/**
 * Adapts {@link ViewOperations} to the {@link TableOperations} interface. It provides a bridge that
 * allows view metadata to be handled in the context of table operations, wrapping the view-specific
 * metadata into a {@link TableMetadata} instance.
 */
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
    throw new UnsupportedOperationException();
  }

  @Override
  public FileIO io() {
    return new DummyFileIO();
  }

  /**
   * Returns the encryption manager for table operations.
   *
   * @return the {@link EncryptionManager} from the default table operations implementation
   */
  @Override
  public EncryptionManager encryption() {
    return TableOperations.super.encryption();
  }

  @Override
  public String metadataFileLocation(String fileName) {
    // Not returning view's metadata location because view metadata uses a different schema compared
    // to table metadata.
    throw new UnsupportedOperationException();
  }

  @Override
  public LocationProvider locationProvider() {
    // Location provider is not supported for view operations.
    throw new UnsupportedOperationException();
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    // Creating temporary table operations is not supported for views.
    throw new UnsupportedOperationException();
  }

  @Override
  public long newSnapshotId() {
    // Generating a new snapshot ID is not supported for view operations.
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean requireStrictCleanup() {
    return false;
  }

  /** Wraps the provided view metadata into a {@link TableMetadata} instance. */
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
