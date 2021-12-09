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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.immutables.value.Value;
import org.mockito.Mockito;

@Value.Immutable
public abstract class MockFileScanTask extends BaseFileScanTask {

  public static MockFileScanTask of(long length) {
    return ImmutableMockFileScanTask.builder()
        .length(length)
        .build();
  }

  public static MockFileScanTask of(DataFile file) {
    return ImmutableMockFileScanTask.builder()
        .file(file)
        .length(file.fileSizeInBytes())
        .build();
  }

  public static MockFileScanTask of(DataFile file, DeleteFile[] deleteFiles) {
    return ImmutableMockFileScanTask.builder()
        .file(file)
        .deletes(Arrays.asList(deleteFiles))
        .length(file.fileSizeInBytes())
        .build();
  }

  public static MockFileScanTask mockTask(long length, int sortOrderId) {
    DataFile mockFile = Mockito.mock(DataFile.class);
    Mockito.when(mockFile.fileSizeInBytes()).thenReturn(length);
    Mockito.when(mockFile.sortOrderId()).thenReturn(sortOrderId);
    return MockFileScanTask.of(mockFile);
  }

  public static MockFileScanTask mockTaskWithDeletes(long length, int nDeletes) {
    DeleteFile[] mockDeletes = new DeleteFile[nDeletes];
    for (int i = 0; i < nDeletes; i++) {
      mockDeletes[i] = Mockito.mock(DeleteFile.class);
    }

    DataFile mockFile = Mockito.mock(DataFile.class);
    Mockito.when(mockFile.fileSizeInBytes()).thenReturn(length);
    return MockFileScanTask.of(mockFile, mockDeletes);
  }

  @Override
  @Nullable
  public abstract DataFile file();

  @Override
  @Nullable
  public abstract List<DeleteFile> deletes();

  @Override
  @Nullable
  public abstract ResidualEvaluator residuals();

  @Override
  @Nullable
  public abstract PartitionSpec spec();

  @Override
  @Nullable
  public abstract Expression residual();

  @Override
  public abstract long length();

  @Override
  public String toString() {
    return "Mock Scan Task Size: " + length();
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
    return length() == that.length();
  }

  @Override
  public int hashCode() {
    return (int) (length() ^ (length() >>> 32));
  }
}
