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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class ParquetObjectRange {
  public CompletableFuture<ByteBuffer> getByteBuffer() {
    return byteBuffer;
  }

  public void setByteBuffer(CompletableFuture<ByteBuffer> byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  private CompletableFuture<ByteBuffer> byteBuffer;
  private long offset;
  private int length;

  public ParquetObjectRange(CompletableFuture<ByteBuffer> byteBuffer, long offset, int length) {
    this.byteBuffer = byteBuffer;
    this.offset = offset;
    this.length = length;
  }
}
