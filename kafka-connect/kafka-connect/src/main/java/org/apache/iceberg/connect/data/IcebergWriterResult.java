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
package org.apache.iceberg.connect.data;

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.types.Types.StructType;

public class IcebergWriterResult {

  private final TableReference tableReference;
  private final List<DataFile> dataFiles;
  private final List<DeleteFile> deleteFiles;
  private final StructType partitionStruct;

  public IcebergWriterResult(
      TableReference tableReference,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles,
      StructType partitionStruct) {
    this.tableReference = tableReference;
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
    this.partitionStruct = partitionStruct;
  }

  /**
   * @deprecated since 1.11.0, will be removed in 1.12.0; use {@link
   *     IcebergWriterResult#IcebergWriterResult(TableReference, List, List, StructType)} instead
   */
  @Deprecated
  public IcebergWriterResult(
      TableIdentifier tableIdentifier,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles,
      StructType partitionStruct) {
    this.tableReference = TableReference.of("unknown", tableIdentifier);
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
    this.partitionStruct = partitionStruct;
  }

  public TableReference tableReference() {
    return tableReference;
  }

  /**
   * @deprecated since 1.11.0, will be removed in 1.12.0; use {@code tableReference().identifier()}
   *     instead
   */
  @Deprecated
  public TableIdentifier tableIdentifier() {
    return tableReference.identifier();
  }

  public List<DataFile> dataFiles() {
    return dataFiles;
  }

  public List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  public StructType partitionStruct() {
    return partitionStruct;
  }
}
