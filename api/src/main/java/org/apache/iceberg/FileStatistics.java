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

import java.nio.ByteBuffer;
import java.util.Map;

public class FileStatistics {

  private long recordCount;

  private Map<Integer, Long> valueCounts;

  private Map<Integer, Long> nullValueCounts;

  private Map<Integer, Long> nanValueCounts;

  private Map<Integer, ByteBuffer> lowerBounds;

  private Map<Integer, ByteBuffer> upperBounds;

  public long getRecordCount() {
    return recordCount;
  }

  public Map<Integer, Long> getValueCounts() {
    return valueCounts;
  }

  public Map<Integer, Long> getNullValueCounts() {
    return nullValueCounts;
  }

  public Map<Integer, Long> getNanValueCounts() {
    return nanValueCounts;
  }

  public Map<Integer, ByteBuffer> getLowerBounds() {
    return lowerBounds;
  }

  public Map<Integer, ByteBuffer> getUpperBounds() {
    return upperBounds;
  }

  public void setRecordCount(long recordCount) {
    this.recordCount = recordCount;
  }

  public void setValueCounts(Map<Integer, Long> valueCounts) {
    this.valueCounts = valueCounts;
  }

  public void setNullValueCounts(Map<Integer, Long> nullValueCounts) {
    this.nullValueCounts = nullValueCounts;
  }

  public void setNanValueCounts(Map nanValueCounts) {
    this.nanValueCounts = nanValueCounts;
  }

  public void setLowerBounds(Map<Integer, ByteBuffer> lowerBounds) {
    this.lowerBounds = lowerBounds;
  }

  public void setUpperBounds(Map<Integer, ByteBuffer> upperBounds) {
    this.upperBounds = upperBounds;
  }

  public FileStatistics(
      long recordCount,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, Long> nanValueCounts,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    this.recordCount = recordCount;
    this.valueCounts = valueCounts;
    this.nullValueCounts = nullValueCounts;
    this.nanValueCounts = nanValueCounts;
    this.lowerBounds = lowerBounds;
    this.upperBounds = upperBounds;
  }
}
