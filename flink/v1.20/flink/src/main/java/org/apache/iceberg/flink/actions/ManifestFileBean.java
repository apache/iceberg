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
package org.apache.iceberg.flink.actions;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

/** A serializable bean that contains a bare minimum to read a manifest. */
public class ManifestFileBean implements ManifestFile, Serializable {

  private String path = null;
  private Long length = null;
  private Integer partitionSpecId = null;
  private Long addedSnapshotId = null;
  private Integer content = null;
  private Long sequenceNumber = null;

  public ManifestFileBean() {}

  public ManifestFileBean(
      String path, long length, int partitionSpecId, int content, long snapshotId) {
    this.path = path;
    this.length = length;
    this.partitionSpecId = partitionSpecId;
    this.content = content;
    this.addedSnapshotId = snapshotId;
    this.sequenceNumber = 0L;
  }

  public static ManifestFileBean fromRowData(StructLike data, Schema schema) {
    Map<String, Integer> pos = getPos(schema);
    return new ManifestFileBean(
        data.get(pos.get("path"), String.class),
        data.get(pos.get("length"), Long.class),
        data.get(pos.get("partition_spec_id"), Integer.class),
        data.get(pos.get("content"), Integer.class),
        data.get(pos.get("added_snapshot_id"), Long.class));
  }

  public static Map<String, Integer> getPos(Schema schema) {
    Map<String, Integer> pos = Maps.newHashMap();
    List<Types.NestedField> columns = schema.columns();
    for (int i = 0; i < columns.size(); i++) {
      pos.put(columns.get(i).name(), i);
    }
    return pos;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Long getLength() {
    return length;
  }

  public void setLength(Long length) {
    this.length = length;
  }

  public Integer getPartitionSpecId() {
    return partitionSpecId;
  }

  public void setPartitionSpecId(Integer partitionSpecId) {
    this.partitionSpecId = partitionSpecId;
  }

  public Long getAddedSnapshotId() {
    return addedSnapshotId;
  }

  public void setAddedSnapshotId(Long addedSnapshotId) {
    this.addedSnapshotId = addedSnapshotId;
  }

  public Integer getContent() {
    return content;
  }

  public void setContent(Integer content) {
    this.content = content;
  }

  public Long getSequenceNumber() {
    return sequenceNumber;
  }

  public void setSequenceNumber(Long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public int partitionSpecId() {
    return partitionSpecId;
  }

  @Override
  public ManifestContent content() {
    return ManifestContent.fromId(content);
  }

  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public long minSequenceNumber() {
    return 0;
  }

  @Override
  public Long snapshotId() {
    return addedSnapshotId;
  }

  @Override
  public Integer addedFilesCount() {
    return null;
  }

  @Override
  public Long addedRowsCount() {
    return null;
  }

  @Override
  public Integer existingFilesCount() {
    return null;
  }

  @Override
  public Long existingRowsCount() {
    return null;
  }

  @Override
  public Integer deletedFilesCount() {
    return null;
  }

  @Override
  public Long deletedRowsCount() {
    return null;
  }

  @Override
  public List<PartitionFieldSummary> partitions() {
    return null;
  }

  @Override
  public ByteBuffer keyMetadata() {
    return null;
  }

  @Override
  public ManifestFile copy() {
    throw new UnsupportedOperationException("Cannot copy");
  }
}
