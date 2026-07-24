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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestBaseDeleteLoader {

  private static final String DATA_FILE = "/tmp/data.parquet";

  @ParameterizedTest
  @MethodSource("invalidDVs")
  public void loadPositionDeletesRejectsInvalidOffsetOrSize(
      Long offset, Long size, String expectedMessage) {
    DeleteFile dv = dv(offset, size);
    DeleteLoader loader = new BaseDeleteLoader(file -> failingInputFile());

    assertThatThrownBy(() -> loader.loadPositionDeletes(ImmutableList.of(dv), DATA_FILE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  private static Stream<Arguments> invalidDVs() {
    return Stream.of(
        Arguments.of(-1L, 10L, "offset must be non-negative"),
        Arguments.of(0L, -1L, "length must be non-negative"));
  }

  // Returns a DV mock reporting the given (offset, size). A mock bypasses the FileMetadata builder,
  // which now rejects negative values at construction time, so it stands in for a corrupted
  // manifest.
  private static DeleteFile dv(Long offset, Long size) {
    DeleteFile dv = mock(DeleteFile.class);
    when(dv.format()).thenReturn(FileFormat.PUFFIN);
    when(dv.location()).thenReturn("/tmp/dv.puffin");
    when(dv.referencedDataFile()).thenReturn(DATA_FILE);
    when(dv.contentOffset()).thenReturn(offset);
    when(dv.contentSizeInBytes()).thenReturn(size);
    return dv;
  }

  // Validation must fire before any I/O. If it does not, the loader calls this and the test fails
  // with a clear message instead of an obscure stack trace.
  private static InputFile failingInputFile() {
    throw new AssertionError("DV validation should reject the invalid metadata before any I/O");
  }
}
