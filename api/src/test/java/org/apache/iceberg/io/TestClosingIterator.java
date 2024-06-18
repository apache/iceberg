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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

public class TestClosingIterator {
  @Test
  public void testEmptyIterator() {
    CloseableIterator<String> underlying = mock(CloseableIterator.class);
    ClosingIterator<String> closingIterator = new ClosingIterator<>(underlying);
    assertThat(closingIterator).isExhausted();
  }

  @Test
  public void testHasNextAndNext() {
    CloseableIterator<String> underlying = mock(CloseableIterator.class);
    when(underlying.hasNext()).thenReturn(true);
    when(underlying.next()).thenReturn("hello");
    ClosingIterator<String> closingIterator = new ClosingIterator<>(underlying);
    assertThat(closingIterator).hasNext();
    assertThat(closingIterator.next()).isEqualTo("hello");
  }

  @Test
  public void testUnderlyingIteratorCloseWhenElementsAreExhausted() throws Exception {
    CloseableIterator<String> underlying = mock(CloseableIterator.class);
    when(underlying.hasNext()).thenReturn(true).thenReturn(false);
    when(underlying.next()).thenReturn("hello");
    ClosingIterator<String> closingIterator = new ClosingIterator<>(underlying);
    assertThat(closingIterator).hasNext();
    assertThat(closingIterator.next()).isEqualTo("hello");
    assertThat(closingIterator).isExhausted();
    verify(underlying, times(1)).close();
  }

  @Test
  public void testCloseCalledOnceForMultipleHasNextCalls() throws Exception {
    CloseableIterator<String> underlying = mock(CloseableIterator.class);
    ClosingIterator<String> closingIterator = new ClosingIterator<>(underlying);
    assertThat(closingIterator).isExhausted();
    verify(underlying, times(1)).close();
  }

  @Test
  public void transformNullCheck() {
    assertThatThrownBy(() -> CloseableIterator.transform(CloseableIterator.empty(), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid transform: null");
  }
}
