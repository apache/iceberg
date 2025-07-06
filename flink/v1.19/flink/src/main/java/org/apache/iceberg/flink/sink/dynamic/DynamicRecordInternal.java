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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Objects;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;

@Internal
class DynamicRecordInternal {

  private String tableName;
  private String branch;
  private Schema schema;
  private PartitionSpec spec;
  private int writerKey;
  private RowData rowData;
  private boolean upsertMode;
  private Set<Integer> equalityFieldIds;

  // Required for serialization instantiation
  DynamicRecordInternal() {}

  DynamicRecordInternal(
      String tableName,
      String branch,
      Schema schema,
      RowData rowData,
      PartitionSpec spec,
      int writerKey,
      boolean upsertMode,
      Set<Integer> equalityFieldsIds) {
    this.tableName = tableName;
    this.branch = branch;
    this.schema = schema;
    this.spec = spec;
    this.writerKey = writerKey;
    this.rowData = rowData;
    this.upsertMode = upsertMode;
    this.equalityFieldIds = equalityFieldsIds;
  }

  public String tableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String branch() {
    return branch;
  }

  public void setBranch(String branch) {
    this.branch = branch;
  }

  public Schema schema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public RowData rowData() {
    return rowData;
  }

  public void setRowData(RowData rowData) {
    this.rowData = rowData;
  }

  public PartitionSpec spec() {
    return spec;
  }

  public void setSpec(PartitionSpec spec) {
    this.spec = spec;
  }

  public int writerKey() {
    return writerKey;
  }

  public void setWriterKey(int writerKey) {
    this.writerKey = writerKey;
  }

  public boolean upsertMode() {
    return upsertMode;
  }

  public void setUpsertMode(boolean upsertMode) {
    this.upsertMode = upsertMode;
  }

  public Set<Integer> equalityFields() {
    return equalityFieldIds;
  }

  public void setEqualityFieldIds(Set<Integer> equalityFieldIds) {
    this.equalityFieldIds = equalityFieldIds;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        tableName, branch, schema, spec, writerKey, rowData, upsertMode, equalityFieldIds);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    DynamicRecordInternal that = (DynamicRecordInternal) other;
    boolean tableFieldsMatch =
        Objects.equals(tableName, that.tableName)
            && Objects.equals(branch, that.branch)
            && schema.schemaId() == that.schema.schemaId()
            && Objects.equals(spec, that.spec)
            && writerKey == that.writerKey
            && upsertMode == that.upsertMode
            && Objects.equals(equalityFieldIds, that.equalityFieldIds);
    if (!tableFieldsMatch) {
      return false;
    }

    if (rowData.getClass().equals(that.rowData.getClass())) {
      return Objects.equals(rowData, that.rowData);
    } else {
      RowDataSerializer rowDataSerializer = new RowDataSerializer(FlinkSchemaUtil.convert(schema));
      return rowDataSerializer
          .toBinaryRow(rowData)
          .equals(rowDataSerializer.toBinaryRow(that.rowData));
    }
  }
}
