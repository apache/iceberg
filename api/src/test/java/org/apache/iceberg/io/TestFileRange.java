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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class TestFileRange {

  @Test
  public void testConstructorRejectsNegativeLength() {
    assertThatThrownBy(
            () -> new FileRange(CompletableFuture.completedFuture(ByteBuffer.allocate(0)), 0L, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid length: -1 in range (must be >= 0)");
  }

  @Test
  public void testConstructorRejectsNegativeOffset() {
    assertThatThrownBy(
            () -> new FileRange(CompletableFuture.completedFuture(ByteBuffer.allocate(0)), -1L, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid offset: -1 in range (must be >= 0)");
  }
}
