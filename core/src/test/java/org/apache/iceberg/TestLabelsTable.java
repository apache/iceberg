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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLabelsTable {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;
  private Table table;

  @BeforeEach
  public void createTable() {
    this.table =
        TABLES.create(
            SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableDir.toURI().toString());
  }

  @Test
  public void labelsTableResolvesThroughMetadataTableUtils() {
    Table labelsTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.LABELS);

    assertThat(labelsTable).isInstanceOf(LabelsTable.class);
    assertThat(labelsTable.schema().findField("scope")).isNotNull();
    assertThat(labelsTable.schema().findField("field_id")).isNotNull();
    assertThat(labelsTable.schema().findField("field_name")).isNotNull();
    assertThat(labelsTable.schema().findField("key")).isNotNull();
    assertThat(labelsTable.schema().findField("value")).isNotNull();
  }

  @Test
  public void scanYieldsNoRowsWhenNoLabels() throws Exception {
    Table labelsTable = new LabelsTable(table);

    List<StructLike> rows = ImmutableList.of();
    try (CloseableIterable<FileScanTask> tasks = labelsTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
          rows = ImmutableList.copyOf(taskRows);
        }
      }
    }

    // a HadoopTables table carries no catalog-provided labels
    assertThat(rows).isEmpty();
  }
}
