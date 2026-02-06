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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestScanTaskUtil {

  @Test
  public void testContentSize() {
    DeleteFile dv1 = mockDV("dv1.puffin", 20L, 25L, "data1.parquet");
    DeleteFile dv2 = mockDV("dv2.puffin", 4L, 15L, "data2.parquet");

    long size1 = ScanTaskUtil.contentSizeInBytes(ImmutableList.of());
    assertThat(size1).isEqualTo(0);

    long size2 = ScanTaskUtil.contentSizeInBytes(ImmutableList.of(dv1));
    assertThat(size2).isEqualTo(25L);

    long size3 = ScanTaskUtil.contentSizeInBytes(ImmutableList.of(dv1, dv2));
    assertThat(size3).isEqualTo(40L);
  }

  private static DeleteFile mockDV(
      String location, long contentOffset, long contentSize, String referencedDataFile) {
    DeleteFile mockFile = Mockito.mock(DeleteFile.class);
    Mockito.when(mockFile.format()).thenReturn(FileFormat.PUFFIN);
    Mockito.when(mockFile.location()).thenReturn(location);
    Mockito.when(mockFile.contentOffset()).thenReturn(contentOffset);
    Mockito.when(mockFile.contentSizeInBytes()).thenReturn(contentSize);
    Mockito.when(mockFile.referencedDataFile()).thenReturn(referencedDataFile);
    return mockFile;
  }
}
