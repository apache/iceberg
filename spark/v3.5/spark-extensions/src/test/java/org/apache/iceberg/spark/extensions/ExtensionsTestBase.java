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
package org.apache.iceberg.spark.extensions;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.TestBase;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class ExtensionsTestBase extends CatalogTestBase {

  private static final Random RANDOM = ThreadLocalRandom.current();

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
            {
                    SparkCatalogConfig.HADOOP.catalogName(),
                    SparkCatalogConfig.HADOOP.implementation(),
                    SparkCatalogConfig.HADOOP.properties()
            },
    };
  }


  @BeforeAll
  public static void startMetastoreAndSpark() {
    TestBase.metastore = new TestHiveMetastore();
    metastore.start();
    TestBase.hiveConf = metastore.hiveConf();

    TestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.testing", "true")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .config(
                SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), String.valueOf(RANDOM.nextBoolean()))
            .enableHiveSupport()
            .getOrCreate();

    TestBase.catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
  }
}
