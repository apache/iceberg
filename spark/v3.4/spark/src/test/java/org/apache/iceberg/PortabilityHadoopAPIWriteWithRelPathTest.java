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
package org.apache.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class PortabilityHadoopAPIWriteWithRelPathTest {
  SparkSession spark;
  String warehouseLocation = "file:///opt/warehouse/my-namespace";

  PortabilityHadoopAPIWriteWithRelPathTest(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("IcebergWriteModeScenarios");

    this.spark =
        SparkSession.builder()
            .config(conf)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.adaptive.enabled", "false")
            // .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();
    initializeHadoopCatalog("hadoop", warehouseLocation);
    // System.out.println(spark.conf().get("spark.eventLog.dir"));
  }

  public static void main(String[] args) {
    new PortabilityHadoopAPIWriteWithRelPathTest(args).run();
  }

  private void run() {
    Catalog catalog = getHadoopCatalog(warehouseLocation);
    TableIdentifier name = TableIdentifier.of("profile", "opportunity");
    Table table = catalog.loadTable(name);

    write(table);
  }

  private void initializeHadoopCatalog(String catalogName, String warehouseLocation) {
    String catalogPrefix = "spark.sql.catalog." + catalogName;
    spark.conf().set(catalogPrefix, "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set(catalogPrefix + ".warehouse", warehouseLocation);
    spark.conf().set(catalogPrefix + ".type", "hadoop");
  }

  private Catalog getHadoopCatalog(String warehouseLocation) {
    HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehouseLocation);
    return hadoopCatalog;
  }

  private void write(Table table) {
    UnboundPredicate predicate = Expressions.equal("device_id", 5);
    DeleteFiles deleteFiles1 = table.newDelete();
    DeleteFiles deleteFiles2 = deleteFiles1.deleteFromRowFilter(predicate);
    deleteFiles2.commit();
  }
}
