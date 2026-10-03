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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class CustomSparkTestBase {

  protected static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;

  @BeforeAll
  public static void startMetastore() {
    metastore = new TestHiveMetastore();
    metastore.start();
    hiveConf = metastore.hiveConf();
  }

  @AfterAll
  public static void stopMetastore() throws Exception {
    if (metastore != null) {
      metastore.stop();
      metastore = null;
    }
  }

  protected SparkSession.Builder baseBuilder() {
    Map<String, Object> disableUIConfig =
        ImmutableMap.of(
            "spark.ui.enabled",
            "false",
            "spark.metrics.conf.*.sink.servlet.class",
            "org.apache.iceberg.spark.DummyMetricsServlet");
    return SparkSession.builder()
        .master("local[1]")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config(disableUIConfig)
        .appName("icebergCustomSparkTest")
        .enableHiveSupport();
  }

  protected void withCustomSpark(Map<String, String> overrides, Consumer<SparkSession> test)
      throws Exception {
    assertThat(SparkSession.getActiveSession().isEmpty())
        .withFailMessage("A Spark session is already active!")
        .isTrue();
    SparkSession.Builder sparkBuilder = baseBuilder();
    overrides.forEach((sparkBuilder::config));
    try (var sparkSession = sparkBuilder.create()) {
      test.accept(sparkSession);
    }
  }
}
