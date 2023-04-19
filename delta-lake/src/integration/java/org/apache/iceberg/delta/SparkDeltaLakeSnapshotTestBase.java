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
package org.apache.iceberg.delta;

import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@SuppressWarnings("VisibilityModifier")
public abstract class SparkDeltaLakeSnapshotTestBase {
  protected static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;
  protected static SparkSession spark = null;

  @BeforeClass
  public static void startMetastoreAndSpark() {
    SparkDeltaLakeSnapshotTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    SparkDeltaLakeSnapshotTestBase.hiveConf = metastore.hiveConf();

    SparkDeltaLakeSnapshotTestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config(
                "spark.hadoop." + HiveConf.ConfVars.METASTOREURIS.varname,
                hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname))
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .enableHiveSupport()
            .getOrCreate();
  }

  @AfterClass
  public static void stopMetastoreAndSpark() throws Exception {
    if (metastore != null) {
      metastore.stop();
      SparkDeltaLakeSnapshotTestBase.metastore = null;
    }
    if (spark != null) {
      spark.stop();
      SparkDeltaLakeSnapshotTestBase.spark = null;
    }
  }

  public SparkDeltaLakeSnapshotTestBase(
      String catalogName, String implementation, Map<String, String> config) {

    spark.conf().set("spark.sql.catalog." + catalogName, implementation);
    config.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog." + catalogName + "." + key, value));
  }
}
