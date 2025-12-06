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
package org.apache.iceberg.connect.data;

import java.time.OffsetDateTime;
import java.util.Objects;

public class Offset implements Comparable<Offset> {

  public static final Offset NULL_OFFSET = new Offset(null, null, null);

  private final Long startOffset;
  private final Long endOffset;
  private final OffsetDateTime timestamp;

  public Offset(Long startOffset, Long endOffset, OffsetDateTime timestamp) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.timestamp = timestamp;
  }

  public Long startOffset() {
    return startOffset;
  }

  public Long endOffset() {
    return endOffset;
  }

  // For backward compatibility - returns the end offset
  public Long offset() {
    return endOffset;
  }

  public OffsetDateTime timestamp() {
    return timestamp;
  }

  @Override
  public int compareTo(Offset other) {
    if (Objects.equals(this.endOffset, other.endOffset)) {
      return 0;
    }
    if (this.endOffset == null || (other.endOffset != null && other.endOffset > this.endOffset)) {
      return -1;
    }
    return 1;
  }
}
