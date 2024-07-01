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
package org.apache.iceberg.spark.actions;

import org.apache.iceberg.FileContent;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class PartitionEntryBean {
  public static final Encoder<PartitionEntryBean> ENCODER = Encoders.bean(PartitionEntryBean.class);

  private FileContent content;
  private int specId;
  // Using Json string representation instead of `PartitionData` object due to below error.
  // Caused by: org.apache.spark.SparkUnsupportedOperationException: Cannot have circular
  // references in bean class, but got the circular reference of class class
  // org.apache.avro.Schema.
  private String partition;
  private long recordCount;
  private long fileSizeInBytes;
  private long lastUpdatedAt;
  private long lastUpdatedSnapshotId;

  public PartitionEntryBean(
      FileContent content,
      int specId,
      String partition,
      long recordCount,
      long fileSizeInBytes,
      long lastUpdatedAt,
      long lastUpdatedSnapshotId) {
    this.content = content;
    this.specId = specId;
    this.partition = partition;
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.lastUpdatedAt = lastUpdatedAt;
    this.lastUpdatedSnapshotId = lastUpdatedSnapshotId;
  }

  public PartitionEntryBean() {}

  // Note: Getter and Setter naming conventions has to be as per Java standard for Spark codegen.

  public FileContent getContent() {
    return content;
  }

  public void setContent(FileContent content) {
    this.content = content;
  }

  public int getSpecId() {
    return specId;
  }

  public void setSpecId(int specId) {
    this.specId = specId;
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

  public long getLastUpdatedAt() {
    return lastUpdatedAt;
  }

  public void setLastUpdatedAt(long lastUpdatedAt) {
    this.lastUpdatedAt = lastUpdatedAt;
  }

  public long getLastUpdatedSnapshotId() {
    return lastUpdatedSnapshotId;
  }

  public void setLastUpdatedSnapshotId(long lastUpdatedSnapshotId) {
    this.lastUpdatedSnapshotId = lastUpdatedSnapshotId;
  }
}
