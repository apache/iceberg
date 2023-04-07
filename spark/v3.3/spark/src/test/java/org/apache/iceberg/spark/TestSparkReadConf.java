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
package org.apache.iceberg.spark;

import org.apache.iceberg.StreamingOverwriteSnapshotReadMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TestSparkReadConf extends SparkTestBaseWithCatalog {

  @Before
  public void before() {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date, days(ts))",
        tableName);
  }

  @After
  public void after() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testStreamingOverwriteSnapshotReadModeDefaultShouldBeBreak() {
    Table table = validationCatalog.loadTable(tableIdent);
    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    Assertions.assertEquals(
        StreamingOverwriteSnapshotReadMode.BREAK, readConf.streamingOverwriteSnapshotsReadMode());
  }

  @Test
  public void testStreamingOverwriteSnapshotReadModeDefaultShouldConsiderSkipOverwritesOption() {
    Table table = validationCatalog.loadTable(tableIdent);
    SparkReadConf readConf =
        new SparkReadConf(
            spark,
            table,
            ImmutableMap.of(SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS, "true"));
    Assertions.assertEquals(
        StreamingOverwriteSnapshotReadMode.SKIP, readConf.streamingOverwriteSnapshotsReadMode());
  }

  @Test
  public void testStreamingOverwriteSnapshotReadModeDefaultShouldWinOverOlderConf() {
    Table table = validationCatalog.loadTable(tableIdent);
    SparkReadConf readConf =
        new SparkReadConf(
            spark,
            table,
            ImmutableMap.of(
                SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS, "true",
                SparkReadOptions.STREAMING_OVERWRITE_SNAPSHOTS_READ_MODE, "BREAK"));
    Assertions.assertEquals(
        StreamingOverwriteSnapshotReadMode.BREAK, readConf.streamingOverwriteSnapshotsReadMode());
  }

  @Test
  public void testStreamingOverwriteSnapshotReadModeDefaultShouldParseRegardlessOfCasing() {
    Table table = validationCatalog.loadTable(tableIdent);
    SparkReadConf readConf =
        new SparkReadConf(
            spark,
            table,
            ImmutableMap.of(
                SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS, "true",
                SparkReadOptions.STREAMING_OVERWRITE_SNAPSHOTS_READ_MODE, "ADDED_files_ONLY"));
    Assertions.assertEquals(
        StreamingOverwriteSnapshotReadMode.ADDED_FILES_ONLY,
        readConf.streamingOverwriteSnapshotsReadMode());
  }
}
