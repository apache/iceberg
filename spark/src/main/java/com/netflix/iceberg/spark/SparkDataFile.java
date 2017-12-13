/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark;

import java.util.Map;

public class SparkDataFile {
  private String path = null;
  private String format = null;
  private String partition = null;
  private long recordCount = 0;
  private long fileSizeInBytes = 0;
  private long blockSizeInBytes = 0;
  private Map<Integer, Long> columnSizes = null;
  private Map<Integer, Long> valueCounts = null;
  private Map<Integer, Long> nullValueCounts = null;
  private Map<Integer, Long> distinctCounts = null;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(long recordCount) {
    this.recordCount = recordCount;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public void setFileSizeInBytes(long fileSizeInBytes) {
    this.fileSizeInBytes = fileSizeInBytes;
  }

  public long getBlockSizeInBytes() {
    return blockSizeInBytes;
  }

  public void setBlockSizeInBytes(long blockSizeInBytes) {
    this.blockSizeInBytes = blockSizeInBytes;
  }

  public Map<Integer, Long> getColumnSizes() {
    return columnSizes;
  }

  public void setColumnSizes(Map<Integer, Long> columnSizes) {
    this.columnSizes = columnSizes;
  }

  public Map<Integer, Long> getValueCounts() {
    return valueCounts;
  }

  public void setValueCounts(Map<Integer, Long> valueCounts) {
    this.valueCounts = valueCounts;
  }

  public Map<Integer, Long> getNullValueCounts() {
    return nullValueCounts;
  }

  public void setNullValueCounts(Map<Integer, Long> nullValueCounts) {
    this.nullValueCounts = nullValueCounts;
  }

  public Map<Integer, Long> getDistinctCounts() {
    return distinctCounts;
  }

  public void setDistinctCounts(Map<Integer, Long> distinctCounts) {
    this.distinctCounts = distinctCounts;
  }
}
