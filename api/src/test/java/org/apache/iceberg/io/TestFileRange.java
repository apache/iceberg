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

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class TestFileRange {

  @Test
  public void validRange() throws EOFException {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    FileRange range = new FileRange(future, 10L, 100);
    assertThat(range.offset()).isEqualTo(10L);
    assertThat(range.length()).isEqualTo(100);
    assertThat(range.byteBuffer()).isSameAs(future);
  }

  @Test
  public void negativeLength() {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    assertThatThrownBy(() -> new FileRange(future, 0L, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid length: -1 in range (must be >= 0)");
  }

  @Test
  public void negativeOffset() {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    assertThatThrownBy(() -> new FileRange(future, -1L, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid offset: -1 in range (must be >= 0)");
  }

  @Test
  public void nullByteBuffer() {
    assertThatThrownBy(() -> new FileRange(null, 0L, 0))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("byteBuffer can't be null");
  }
}
