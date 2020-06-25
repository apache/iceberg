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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public class SparkTestBase {

  private static TestHiveMetastore metastore = null;
  private static HiveConf hiveConf = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;

  @BeforeClass
  public static void startMetastoreAndSpark() {
    SparkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    SparkTestBase.hiveConf = metastore.hiveConf();

    SparkTestBase.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .enableHiveSupport()
        .getOrCreate();

    SparkTestBase.catalog = new HiveCatalog(spark.sessionState().newHadoopConf());
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    catalog.close();
    SparkTestBase.catalog = null;
    metastore.stop();
    SparkTestBase.metastore = null;
    spark.stop();
    SparkTestBase.spark = null;
  }

  protected static String dbPath(String dbName) {
    return metastore.getDatabasePath(dbName);
  }
}
