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
import static org.apache.iceberg.spark.SparkTestHelperBase.rowsToJava;

import java.util.List;
import java.util.Scanner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;

public final class SparkQueryRunner {
  private static final Joiner JOIN = Joiner.on("|");

  private SparkQueryRunner() {}

  public static void main(String[] args) throws Exception {
    TestHiveMetastore metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .config("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName())
            .enableHiveSupport()
            .getOrCreate();

    Scanner sc = new Scanner(System.in);
    while (true) {
      System.out.print("spark-sql> ");
      try {
        Dataset<Row> result = spark.sql(sc.nextLine());
        List<Object[]> rows = rowsToJava(result.collectAsList());

        String header = JOIN.join(result.columns());
        System.out.println(header);
        System.out.println("=".repeat(header.length()));
        for (Object[] row : rows) {
          System.out.println(JOIN.join(row));
        }
      } catch (Throwable e) {
        System.err.printf("ERROR: %s", e);
      }
      System.out.println();
    }
  }
}
