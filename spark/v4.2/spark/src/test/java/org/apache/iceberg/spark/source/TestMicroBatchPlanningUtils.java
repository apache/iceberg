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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMicroBatchPlanningUtils extends CatalogTestBase {

  private Table table;

  @BeforeEach
  public void setupTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s "
            + "(id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(3, id))",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testUnpackedLimitsCompositeChoosesMinimum() {
    ReadLimit[] limits =
        new ReadLimit[] {
          ReadLimit.maxRows(10), ReadLimit.maxRows(4), ReadLimit.maxFiles(8), ReadLimit.maxFiles(2)
        };

    ReadLimit composite = ReadLimit.compositeLimit(limits);

    BaseSparkMicroBatchPlanner.UnpackedLimits unpacked =
        new BaseSparkMicroBatchPlanner.UnpackedLimits(composite);

    assertThat(unpacked.getMaxRows()).isEqualTo(4);
    assertThat(unpacked.getMaxFiles()).isEqualTo(2);
  }

  @TestTemplate
  public void testDetermineStartingOffsetWithTimestampBetweenSnapshots() {
    sql("INSERT INTO %s VALUES (1, 'one')", tableName);
    table.refresh();
    long snapshot1Time = table.currentSnapshot().timestampMillis();

    sql("INSERT INTO %s VALUES (2, 'two')", tableName);
    table.refresh();
    long snapshot2Id = table.currentSnapshot().snapshotId();

    StreamingOffset offset = MicroBatchUtils.determineStartingOffset(table, snapshot1Time + 1);

    assertThat(offset.snapshotId()).isEqualTo(snapshot2Id);
    assertThat(offset.position()).isEqualTo(0L);
    assertThat(offset.shouldScanAllFiles()).isFalse();
  }

  @TestTemplate
  public void testAddedFilesCountUsesSummaryWhenPresent() {
    sql("INSERT INTO %s VALUES (1, 'one')", tableName);
    table.refresh();

    long expectedAddedFiles =
        Long.parseLong(table.currentSnapshot().summary().get(SnapshotSummary.ADDED_FILES_PROP));

    long actual = MicroBatchUtils.addedFilesCount(table, table.currentSnapshot());

    assertThat(actual).isEqualTo(expectedAddedFiles);
  }
}
