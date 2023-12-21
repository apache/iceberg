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
import org.apache.iceberg.StructLike;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class PartitionEntryBean {
  public static final Encoder<PartitionEntryBean> ENCODER = Encoders.bean(PartitionEntryBean.class);

  private FileContent content;
  private int specId;
  private StructLike partition;
  private long recordCount;
  private long fileSizeInBytes;
  private long lastUpdatedAt;
  private long lastUpdatedSnapshotId;

  public PartitionEntryBean(
      FileContent content,
      int specId,
      StructLike partition,
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

  public FileContent content() {
    return content;
  }

  public int specId() {
    return specId;
  }

  public StructLike partition() {
    return partition;
  }

  public long recordCount() {
    return recordCount;
  }

  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  public long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  public long lastUpdatedSnapshotId() {
    return lastUpdatedSnapshotId;
  }
}
