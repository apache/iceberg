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

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;

/** A DynamicRecord contains RowData alongside with the Iceberg table metadata. */
public class DynamicRecord {

  private TableIdentifier tableIdentifier;
  private String branch;
  private Schema schema;
  private RowData rowData;
  private PartitionSpec partitionSpec;
  private DistributionMode distributionMode;
  private int writeParallelism;
  private boolean upsertMode;
  @Nullable private Set<String> equalityFields;

  public DynamicRecord(
      TableIdentifier tableIdentifier,
      String branch,
      Schema schema,
      RowData rowData,
      PartitionSpec partitionSpec,
      DistributionMode distributionMode,
      int writeParallelism) {
    this.tableIdentifier = tableIdentifier;
    this.branch = branch;
    this.schema = schema;
    this.partitionSpec = partitionSpec;
    this.rowData = rowData;
    this.distributionMode = distributionMode;
    this.writeParallelism = writeParallelism;
  }

  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }

  public void setTableIdentifier(TableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
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

  public PartitionSpec spec() {
    return partitionSpec;
  }

  public void setPartitionSpec(PartitionSpec partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  public RowData rowData() {
    return rowData;
  }

  public void setRowData(RowData rowData) {
    this.rowData = rowData;
  }

  public DistributionMode distributionMode() {
    return distributionMode;
  }

  public void setDistributionMode(DistributionMode distributionMode) {
    this.distributionMode = distributionMode;
  }

  public int writeParallelism() {
    return writeParallelism;
  }

  public void writeParallelism(int parallelism) {
    this.writeParallelism = parallelism;
  }

  public boolean upsertMode() {
    return upsertMode;
  }

  public void setUpsertMode(boolean upsertMode) {
    this.upsertMode = upsertMode;
  }

  public Set<String> equalityFields() {
    return equalityFields;
  }

  public void setEqualityFields(Set<String> equalityFields) {
    this.equalityFields = equalityFields;
  }
}
