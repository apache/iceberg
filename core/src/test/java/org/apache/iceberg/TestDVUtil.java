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

import org.junit.jupiter.api.Test;

public class TestDVUtil {

  @Test
  public void validateDVRejectsNullOffset() {
    DeleteFile dv = dv(null, 10L);
    assertThatThrownBy(() -> DVUtil.validateDV(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("offset cannot be null");
  }

  @Test
  public void validateDVRejectsNullLength() {
    DeleteFile dv = dv(0L, null);
    assertThatThrownBy(() -> DVUtil.validateDV(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("length cannot be null");
  }

  @Test
  public void validateDVRejectsNegativeOffset() {
    DeleteFile dv = dv(-1L, 10L);
    assertThatThrownBy(() -> DVUtil.validateDV(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("offset must be non-negative");
  }

  @Test
  public void validateDVRejectsNegativeLength() {
    DeleteFile dv = dv(0L, -1L);
    assertThatThrownBy(() -> DVUtil.validateDV(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("length must be non-negative");
  }

  @Test
  public void validateDVRejectsLengthEqualToIntegerMax() {
    DeleteFile dv = dv(0L, (long) Integer.MAX_VALUE);
    assertThatThrownBy(() -> DVUtil.validateDV(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can't read DV larger than 2GB");
  }

  @Test
  public void validateDVRejectsLengthAboveIntegerMax() {
    DeleteFile dv = dv(0L, (long) Integer.MAX_VALUE + 1);
    assertThatThrownBy(() -> DVUtil.validateDV(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can't read DV larger than 2GB");
  }

  @Test
  public void validateDVAcceptsZero() {
    DeleteFile dv = dv(0L, 0L);
    assertThatCode(() -> DVUtil.validateDV(dv)).doesNotThrowAnyException();
  }

  @Test
  public void validateDVAcceptsTypicalValues() {
    DeleteFile dv = dv(4L, 4096L);
    assertThatCode(() -> DVUtil.validateDV(dv)).doesNotThrowAnyException();
  }

  @Test
  public void validateDVAcceptsMaximumLength() {
    DeleteFile dv = dv(0L, (long) Integer.MAX_VALUE - 1);
    assertThatCode(() -> DVUtil.validateDV(dv)).doesNotThrowAnyException();
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
