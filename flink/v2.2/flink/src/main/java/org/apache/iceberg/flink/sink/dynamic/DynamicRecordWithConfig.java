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
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkWriteConf;

class DynamicRecordWithConfig extends DynamicRecord {
  private final String defaultBranch;
  private final Integer defaultWriteParallelism;

  private DynamicRecord wrapped;

  DynamicRecordWithConfig(FlinkWriteConf flinkWriteConf) {
    this.defaultBranch = flinkWriteConf.branch();
    this.defaultWriteParallelism = flinkWriteConf.writeParallelism();
  }

  DynamicRecordWithConfig wrap(DynamicRecord newWrapped) {
    this.wrapped = newWrapped;
    return this;
  }

  @Override
  public String branch() {
    return wrapped.branch() != null ? wrapped.branch() : defaultBranch;
  }

  @Override
  public DistributionMode distributionMode() {
    return wrapped.distributionMode();
  }

  @Override
  public int writeParallelism() {
    int originalParallelism = wrapped.writeParallelism();
    if (originalParallelism > 0 || defaultWriteParallelism == null) {
      return originalParallelism;
    }

    return defaultWriteParallelism;
  }

  @Override
  public TableIdentifier tableIdentifier() {
    return wrapped.tableIdentifier();
  }

  @Override
  public Schema schema() {
    return wrapped.schema();
  }

  @Override
  public PartitionSpec spec() {
    return wrapped.spec();
  }

  @Override
  public RowData rowData() {
    return wrapped.rowData();
  }

  @Override
  public boolean upsertMode() {
    return wrapped.upsertMode();
  }

  @Override
  public Set<String> equalityFields() {
    return wrapped.equalityFields();
  }
}
