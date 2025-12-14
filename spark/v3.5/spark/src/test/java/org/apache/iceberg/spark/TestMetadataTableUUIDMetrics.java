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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("spark")
public class TestMetadataTableUUIDMetrics {

  private static SparkSession spark;

  @TempDir static Path warehouse;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("uuid-readable-metrics-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.test", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.test.type", "hadoop")
            .config("spark.sql.catalog.test.warehouse", warehouse.toString())
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testUUIDMetricsReadableInAllFiles() {
    spark.sql("CREATE TABLE test.uuid_metrics_test (id STRING) USING iceberg");

    List<String> uuids =
        List.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString());

    spark
        .createDataset(uuids, Encoders.STRING())
        .toDF("id")
        .write()
        .format("iceberg")
        .mode("append")
        .save("test.uuid_metrics_test");

    Dataset<Row> df = spark.sql("SELECT readable_metrics FROM test.uuid_metrics_test.all_files");

    List<Row> rows = df.collectAsList();
    assertThat(rows).isNotEmpty();

    // readable_metrics is a STRUCT
    Row readableMetrics = rows.get(0).getStruct(0);

    // column "id" metrics
    Row idMetrics = readableMetrics.getStruct(readableMetrics.fieldIndex("id"));

    assertThat(idMetrics).isNotNull();

    String lower = idMetrics.getString(idMetrics.fieldIndex("lower_bound"));
    String upper = idMetrics.getString(idMetrics.fieldIndex("upper_bound"));

    assertThat(lower).isNotNull();
    assertThat(upper).isNotNull();

    // metrics may be truncated — must not throw or be unreadable
    assertThat(lower).isNotEmpty();
    assertThat(upper).isNotEmpty();

    // lexicographic ordering must still hold
    assertThat(lower.compareTo(upper)).isLessThanOrEqualTo(0);

    // looks like a UUID prefix
    assertThat(lower).matches("[0-9a-fA-F\\-]+");
    assertThat(upper).matches("[0-9a-fA-F\\-]+");
  }
}
