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

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FileRange {
  private final CompletableFuture<ByteBuffer> byteBuffer;
  private final long offset;
  private final int length;

  public FileRange(CompletableFuture<ByteBuffer> byteBuffer, long offset, int length)
      throws EOFException {
    Preconditions.checkNotNull(offset, "offset is null");
    Preconditions.checkNotNull(length, "length is null");
    Preconditions.checkArgument(length() >= 0, "length %s is negative ", length);
    if (offset < 0) {
      throw new EOFException("position is negative in range: " + offset);
    }
    this.byteBuffer = byteBuffer;
    this.offset = offset;
    this.length = length;
  }

  public CompletableFuture<ByteBuffer> byteBuffer() {
    return byteBuffer;
  }

  public long offset() {
    return offset;
  }

  public int length() {
    return length;
  }
}
