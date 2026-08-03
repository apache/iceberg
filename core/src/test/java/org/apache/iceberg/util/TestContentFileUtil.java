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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.DeleteFile;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Test methods in {@link ContentFileUtil}. */
public class TestContentFileUtil {
  @Test
  public void testValidateDVAcceptsUsableOffsetAndLength() {
    // the happy path
    ContentFileUtil.validateDV(mockDV(0L, 100L));
  }

  @Test
  public void testValidateDVRejectsNullOffset() {
    assertThatThrownBy(() -> ContentFileUtil.validateDV(mockDV(null, 100L)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("offset cannot be null");
  }

  @Test
  public void testValidateDVRejectsNullLength() {
    assertThatThrownBy(() -> ContentFileUtil.validateDV(mockDV(0L, null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("length is null");
  }

  @Test
  public void testValidateDVRejectsLengthLargerThan2GB() {
    assertThatThrownBy(() -> ContentFileUtil.validateDV(mockDV(0L, Integer.MAX_VALUE + 1L)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("larger than 2GB");
  }

  /**
   * Create a mock Delete File with the supplied content offset and size; all other methods of the
   * interface will be the default values or undefined.
   *
   * @param contentOffset offset; may be null.
   * @param contentSize content size; may be null.
   * @return a mock delete file.
   */
  private static DeleteFile mockDV(Long contentOffset, Long contentSize) {
    DeleteFile mock = Mockito.mock(DeleteFile.class);
    Mockito.when(mock.contentOffset()).thenReturn(contentOffset);
    Mockito.when(mock.contentSizeInBytes()).thenReturn(contentSize);
    return mock;
  }
}
