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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

/** Use the FlinkSource */
public class TestFlinkSourceSql extends TestSqlBase {
  @Override
  public void before() throws IOException {
    SqlHelpers.sql(
        getTableEnv(),
        "create catalog iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_EXTENSION.warehouse());
    SqlHelpers.sql(getTableEnv(), "use catalog iceberg_catalog");
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }

  @Test
  public void testInferParallelismWithGlobalSetting() throws IOException {
    Configuration cfg = getTableEnv().getConfig().getConfiguration();
    cfg.set(PipelineOptions.MAX_PARALLELISM, 1);

    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, null);

    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
    List<Record> expectedRecords = Lists.newArrayList();
    long maxFileLen = 0;
    for (int i = 0; i < 5; i++) {
      List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 2, i);
      DataFile dataFile = helper.writeFile(null, records);
      helper.appendToTable(dataFile);
      expectedRecords.addAll(records);
      maxFileLen = Math.max(dataFile.fileSizeInBytes(), maxFileLen);
    }

    // Make sure to generate multiple CombinedScanTasks
    SqlHelpers.sql(
        getTableEnv(),
        "ALTER TABLE t SET ('read.split.open-file-cost'='1', 'read.split.target-size'='%s')",
        maxFileLen);

    List<Row> results = run(Maps.newHashMap(), "", "*");
    org.apache.iceberg.flink.TestHelpers.assertRecords(
        results, expectedRecords, TestFixtures.SCHEMA);
  }
}
