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

import org.mockito.Mockito;

public class MockFileScanTask extends BaseFileScanTask {

  private final long length;

  public MockFileScanTask(long length) {
    super(null, null, null, null, null);
    this.length = length;
  }

  public MockFileScanTask(DataFile file) {
    super(file, null, null, null, null);
    this.length = file.fileSizeInBytes();
  }

  public MockFileScanTask(DataFile file, DeleteFile[] deleteFiles) {
    super(file, deleteFiles, null, null, null);
    this.length = file.fileSizeInBytes();
  }

  public MockFileScanTask(DataFile file, String schemaString, String specString) {
    super(file, null, schemaString, specString, null);
    this.length = file.fileSizeInBytes();
  }

  public MockFileScanTask(DataFile file, Schema schema, PartitionSpec spec) {
    super(file, null, SchemaParser.toJson(schema), PartitionSpecParser.toJson(spec), null);
    this.length = file.fileSizeInBytes();
  }

  public MockFileScanTask(
      DataFile file, DeleteFile[] deleteFiles, Schema schema, PartitionSpec spec) {
    super(file, deleteFiles, SchemaParser.toJson(schema), PartitionSpecParser.toJson(spec), null);
    this.length = file.fileSizeInBytes();
  }

  public static MockFileScanTask mockTask(long length, int sortOrderId) {
    DataFile mockFile = Mockito.mock(DataFile.class);
    Mockito.when(mockFile.fileSizeInBytes()).thenReturn(length);
    Mockito.when(mockFile.sortOrderId()).thenReturn(sortOrderId);
    return new MockFileScanTask(mockFile);
  }

  public static MockFileScanTask mockTaskWithDeletes(long length, int nDeletes) {
    DeleteFile[] mockDeletes = new DeleteFile[nDeletes];
    for (int i = 0; i < nDeletes; i++) {
      mockDeletes[i] = Mockito.mock(DeleteFile.class);
    }

    DataFile mockFile = Mockito.mock(DataFile.class);
    Mockito.when(mockFile.fileSizeInBytes()).thenReturn(length);
    return new MockFileScanTask(mockFile, mockDeletes);
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public String toString() {
    return "Mock Scan Task Size: " + length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MockFileScanTask that = (MockFileScanTask) o;
    return length == that.length;
  }

  @Override
  public int hashCode() {
    return (int) (length ^ (length >>> 32));
  }
}
