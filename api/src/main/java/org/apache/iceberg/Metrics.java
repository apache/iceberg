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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

public class Metrics implements Serializable {

  private Long rowCount = null;
  private Map<Integer, Long> columnSizes = null;
  private Map<Integer, Long> valueCounts = null;
  private Map<Integer, Long> nullValueCounts = null;
  private Map<Integer, ByteBuffer> lowerBounds = null;
  private Map<Integer, ByteBuffer> upperBounds = null;

  public Metrics() {
  }

  public Metrics(Long rowCount,
                 Map<Integer, Long> columnSizes,
                 Map<Integer, Long> valueCounts,
                 Map<Integer, Long> nullValueCounts) {
    this.rowCount = rowCount;
    this.columnSizes = columnSizes;
    this.valueCounts = valueCounts;
    this.nullValueCounts = nullValueCounts;
  }

  public Metrics(Long rowCount,
                 Map<Integer, Long> columnSizes,
                 Map<Integer, Long> valueCounts,
                 Map<Integer, Long> nullValueCounts,
                 Map<Integer, ByteBuffer> lowerBounds,
                 Map<Integer, ByteBuffer> upperBounds) {
    this.rowCount = rowCount;
    this.columnSizes = columnSizes;
    this.valueCounts = valueCounts;
    this.nullValueCounts = nullValueCounts;
    this.lowerBounds = lowerBounds;
    this.upperBounds = upperBounds;
  }

  public Long recordCount() {
    return rowCount;
  }

  public Map<Integer, Long> columnSizes() {
    return columnSizes;
  }

  public Map<Integer, Long> valueCounts() {
    return valueCounts;
  }

  public Map<Integer, Long> nullValueCounts() {
    return nullValueCounts;
  }

  public Map<Integer, ByteBuffer> lowerBounds() {
    return lowerBounds;
  }

  public Map<Integer, ByteBuffer> upperBounds() {
    return upperBounds;
  }
}
