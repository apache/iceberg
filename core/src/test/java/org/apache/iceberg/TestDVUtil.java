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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestDVUtil {

  @ParameterizedTest
  @MethodSource("invalidDVs")
  public void validateDVRejectsInvalidOffsetOrLength(
      Long offset, Long length, String expectedMessage) {
    DeleteFile dv = dv(offset, length);
    assertThatThrownBy(() -> DVUtil.validateDV(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  private static Stream<Arguments> invalidDVs() {
    return Stream.of(
        Arguments.of(null, 10L, "offset cannot be null"),
        Arguments.of(0L, null, "length cannot be null"),
        Arguments.of(-1L, 10L, "offset must be non-negative"),
        Arguments.of(0L, -1L, "length must be non-negative"),
        Arguments.of(0L, (long) Integer.MAX_VALUE, "Can't read DV larger than 2GB"),
        Arguments.of(0L, Integer.MAX_VALUE + 1L, "Can't read DV larger than 2GB"));
  }

  @ParameterizedTest
  @MethodSource("validDVs")
  public void validateDVAcceptsValidOffsetAndLength(Long offset, Long length) {
    DeleteFile dv = dv(offset, length);
    assertThatCode(() -> DVUtil.validateDV(dv)).doesNotThrowAnyException();
  }

  private static Stream<Arguments> validDVs() {
    return Stream.of(
        Arguments.of(0L, 0L), Arguments.of(4L, 4096L), Arguments.of(0L, Integer.MAX_VALUE - 1L));
  }

  private static DeleteFile dv(Long offset, Long length) {
    DeleteFile dv = mock(DeleteFile.class);
    when(dv.location()).thenReturn("/tmp/test.puffin");
    when(dv.referencedDataFile()).thenReturn("/tmp/data.parquet");
    when(dv.contentOffset()).thenReturn(offset);
    when(dv.contentSizeInBytes()).thenReturn(length);
    return dv;
  }
}
