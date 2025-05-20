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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.Test;

class TestDynamicWriter extends TestFlinkIcebergSinkBase {

  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("myTable");

  @Test
  void testDynamicWriter() throws Exception {
    runWriterTest();
  }

  private static DynamicWriter runWriterTest() throws Exception {
    return runWriterTest(Map.of());
  }

  private static DynamicWriter runWriterTest(Map<String, String> writeProperties) throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE_IDENTIFIER, SimpleDataUtil.SCHEMA);

    DynamicWriter dynamicWriter =
        new DynamicWriter(
            catalog,
            FileFormat.PARQUET,
            1024L,
            writeProperties,
            100,
            new DynamicWriterMetrics(new UnregisteredMetricsGroup()),
            0,
            0);

    DynamicRecordInternal record = new DynamicRecordInternal();
    record.setTableName(TABLE_IDENTIFIER.name());
    record.setSchema(table.schema());
    record.setSpec(table.spec());
    record.setRowData(SimpleDataUtil.createRowData(1, "test"));

    assertThat(getNumDataFiles(table)).isEqualTo(0);

    dynamicWriter.write(record, null);
    Collection<DynamicWriteResult> writeResults1 = dynamicWriter.prepareCommit();

    assertThat(getNumDataFiles(table)).isEqualTo(1);

    assertThat(writeResults1.size()).isEqualTo(1);
    WriteResult wr1 = writeResults1.iterator().next().writeResult();
    assertThat(wr1.dataFiles().length).isEqualTo(1);
    assertThat(wr1.dataFiles()[0].format()).isEqualTo(FileFormat.PARQUET);
    assertThat(wr1.deleteFiles()).isEmpty();

    dynamicWriter.write(record, null);
    Collection<DynamicWriteResult> writeResults2 = dynamicWriter.prepareCommit();

    assertThat(getNumDataFiles(table)).isEqualTo(2);

    assertThat(writeResults2.size()).isEqualTo(1);
    WriteResult wr2 = writeResults2.iterator().next().writeResult();
    assertThat(wr2.dataFiles().length).isEqualTo(1);
    assertThat(wr2.dataFiles()[0].format()).isEqualTo(FileFormat.PARQUET);
    assertThat(wr2.deleteFiles()).isEmpty();

    dynamicWriter.close();

    return dynamicWriter;
  }

  private static int getNumDataFiles(Table table) {
    File dataDir = new File(URI.create(table.location()).getPath(), "data");
    if (dataDir.exists()) {
      return dataDir.listFiles((dir, name) -> !name.startsWith(".")).length;
    }
    return 0;
  }
}
