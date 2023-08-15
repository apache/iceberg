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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.IcebergSpark;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull;
import org.apache.spark.sql.types.DataTypes;

public class PortabilityHadoopAPIWriteWithRelPathTest {
  SparkSession spark;
  String warehouseLocation = "file:///opt/warehouse/my-namespace";
  int bucketSize = 2;

  boolean populate = false;

  PortabilityHadoopAPIWriteWithRelPathTest(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("IcebergWriteModeScenarios");

    this.spark =
        SparkSession.builder()
            .config(conf)
            //            .config(
            //                "spark.sql.extensions",
            //                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.adaptive.enabled", "false")
            // .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();
    initializeHadoopCatalog("hadoop");
    // System.out.println(spark.conf().get("spark.eventLog.dir"));
  }

  public static void main(String[] args) {
    new PortabilityHadoopAPIWriteWithRelPathTest(args).run();
  }

  private void run() {
    Catalog catalog = getHadoopCatalog();

    if (populate) {
      Table table = createTable(catalog, "profile", "opportunity");
      System.out.println("------------------\n-----------------\n------------------\n");
      insertData(table, "device_data"); // seq=1
      System.out.println("------------------\n-----------------\n------------------\n");
      markTableForRelativePath(table);
    } else {
      TableIdentifier name = TableIdentifier.of("profile", "opportunity");
      Table table = catalog.loadTable(name);
      writeWithRelative(table);
    }
  }

  private void initializeHadoopCatalog(String catalogName) {
    String catalogPrefix = "spark.sql.catalog." + catalogName;
    spark.conf().set(catalogPrefix, "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set(catalogPrefix + ".warehouse", warehouseLocation);
    spark.conf().set(catalogPrefix + ".type", "hadoop");
  }

  private Catalog getHadoopCatalog() {
    HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehouseLocation);
    return hadoopCatalog;
  }

  private void writeWithRelative(Table table) {
    UnboundPredicate predicate = Expressions.equal("device_id", 5);
    DeleteFiles deleteFiles1 = table.newDelete();
    DeleteFiles deleteFiles2 = deleteFiles1.deleteFromRowFilter(predicate);
    deleteFiles2.commit();
  }

  private Table createTable(Catalog catalog, String database, String tableName) {
    if (catalog.tableExists(TableIdentifier.of(database, tableName))) {
      dropTable(catalog, database, tableName);
    }

    List nestedFields = new ArrayList<Types.NestedField>();
    nestedFields.add(required(0, "device_id", Types.IntegerType.get()));
    nestedFields.add(optional(1, "device_name", Types.StringType.get()));

    Set set = new HashSet<Integer>();
    set.add(0);
    Schema schema = new Schema(nestedFields, set);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.format.default", "parquet");
    properties.put("write.target-file-size-bytes", "134217728");
    properties.put("write.parquet.row-group-size-bytes", "134217728");

    return catalog
        .buildTable(TableIdentifier.of(database, tableName), schema)
        .withSortOrder(SortOrder.builderFor(schema).asc("device_id", NullOrder.NULLS_FIRST).build())
        .withProperties(properties)
        .withPartitionSpec(PartitionSpec.builderFor(schema).bucket("device_id", bucketSize).build())
        .
        // withProperty("write.distribution-mode", "hash").
        create();
  }

  private void dropTable(Catalog catalog, String database, String tableName) {
    catalog.dropTable(TableIdentifier.of(database, tableName));
  }

  private void insertData(Table table, String fileName) {
    String inputDataLocation = "file:///opt/warehouse/" + fileName + ".csv";
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket", DataTypes.IntegerType, bucketSize);

    Dataset<Row> inputData =
        spark
            .read()
            .option("header", true)
            .csv(inputDataLocation)
            .withColumn("device_id", col("device_id").cast("integer"))
            .withColumn(
                "device_id",
                new Column(
                    new AssertNotNull(
                        col("device_id").expr(), scala.collection.immutable.List.empty())));
    // repartition(expr("iceberg_bucket(device_id)")).
    // sortWithinPartitions(expr("iceberg_bucket(device_id)"));
    inputData.createOrReplaceTempView(fileName);

    // spark.sql("INSERT INTO "+table.toString()+" SELECT * FROM "+fileName);
    try {
      inputData.writeTo(table.toString()).append();
    } catch (NoSuchTableException e) {
      System.out.println(e.message());
    }
  }

  private void markTableForRelativePath(Table table) {
    table
        .updateProperties()
        .set("write.metadata.use.relative-path", "true")
        .set("prefix", "file:///opt/warehouse")
        .commit();
  }
}
