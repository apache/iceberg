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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class VortexAzureSmokeTest {
  private static final String AZURE_ACCESS_KEY = System.getenv("AZURE_ACCESS_KEY");
  private static final Configuration CONF = new Configuration();
  private static final String WAREHOUSE = "abfss://tpch@vortexicebergsummit25.dfs.core.windows.net";

  private static final Map<String, Long> RECORD_COUNTS;

  private static final Schema LINEITEM_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "l_orderkey", Types.LongType.get()),
          Types.NestedField.required(2, "l_partkey", Types.LongType.get()),
          Types.NestedField.required(3, "l_suppkey", Types.LongType.get()),
          Types.NestedField.required(4, "l_linenumber", Types.LongType.get()),
          Types.NestedField.required(5, "l_quantity", Types.LongType.get()),
          Types.NestedField.required(6, "l_extendedprice", Types.LongType.get()),
          Types.NestedField.required(7, "l_discount", Types.LongType.get()),
          Types.NestedField.required(8, "l_tax", Types.LongType.get()),
          Types.NestedField.required(9, "l_returnflag", Types.StringType.get()),
          Types.NestedField.required(10, "l_linestatus", Types.StringType.get()),
          Types.NestedField.required(11, "l_shipdate", Types.DateType.get()),
          Types.NestedField.required(12, "l_commitdate", Types.DateType.get()),
          Types.NestedField.required(13, "l_receiptdate", Types.DateType.get()),
          Types.NestedField.required(14, "l_shipinstruct", Types.StringType.get()),
          Types.NestedField.required(15, "l_shipmode", Types.StringType.get()),
          Types.NestedField.required(16, "l_comment", Types.StringType.get()));

  private static final Schema ORDERS_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "o_orderkey", Types.LongType.get()),
          Types.NestedField.required(2, "o_custkey", Types.LongType.get()),
          Types.NestedField.required(3, "o_orderstatus", Types.StringType.get()),
          Types.NestedField.required(4, "o_totalprice", Types.LongType.get()),
          Types.NestedField.required(5, "o_orderdate", Types.DateType.get()),
          Types.NestedField.required(6, "o_orderpriority", Types.StringType.get()),
          Types.NestedField.required(7, "o_clerk", Types.StringType.get()),
          Types.NestedField.required(8, "o_shippriority", Types.IntegerType.get()),
          Types.NestedField.required(9, "o_comment", Types.StringType.get()));

  private static final Schema CUSTOMER_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "c_custkey", Types.LongType.get()),
          Types.NestedField.required(2, "c_name", Types.StringType.get()),
          Types.NestedField.required(3, "c_address", Types.StringType.get()),
          Types.NestedField.required(4, "c_nationkey", Types.LongType.get()),
          Types.NestedField.required(5, "c_phone", Types.StringType.get()),
          Types.NestedField.required(6, "c_acctbal", Types.LongType.get()),
          Types.NestedField.required(7, "c_mktsegment", Types.StringType.get()),
          Types.NestedField.required(8, "c_comment", Types.StringType.get()));

  private static final Schema NATION_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "n_nationkey", Types.LongType.get()),
          Types.NestedField.required(2, "n_name", Types.StringType.get()),
          Types.NestedField.required(3, "n_regionkey", Types.LongType.get()),
          Types.NestedField.optional(4, "n_comment", Types.StringType.get()));

  private static final Schema REGION_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "r_regionkey", Types.LongType.get()),
          Types.NestedField.required(2, "r_name", Types.StringType.get()),
          Types.NestedField.optional(3, "r_comment", Types.StringType.get()));

  private static final Schema PART_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "p_partkey", Types.LongType.get()),
          Types.NestedField.required(2, "p_name", Types.StringType.get()),
          Types.NestedField.required(3, "p_mfgr", Types.StringType.get()),
          Types.NestedField.required(4, "p_brand", Types.StringType.get()),
          Types.NestedField.required(5, "p_type", Types.StringType.get()),
          Types.NestedField.required(6, "p_size", Types.IntegerType.get()),
          Types.NestedField.required(7, "p_container", Types.StringType.get()),
          Types.NestedField.required(8, "p_retailprice", Types.LongType.get()),
          Types.NestedField.required(9, "p_comment", Types.StringType.get()));

  private static final Schema SUPPLIER_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "s_suppkey", Types.LongType.get()),
          Types.NestedField.required(2, "s_name", Types.StringType.get()),
          Types.NestedField.required(3, "s_address", Types.StringType.get()),
          Types.NestedField.required(4, "s_nationkey", Types.IntegerType.get()),
          Types.NestedField.required(5, "s_phone", Types.StringType.get()),
          Types.NestedField.required(6, "s_acctbal", Types.LongType.get()),
          Types.NestedField.required(7, "s_comment", Types.StringType.get()));

  private static final Schema PARTSUPP_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "ps_partkey", Types.LongType.get()),
          Types.NestedField.required(2, "ps_suppkey", Types.LongType.get()),
          Types.NestedField.required(3, "ps_availqty", Types.LongType.get()),
          Types.NestedField.required(4, "ps_supplycost", Types.LongType.get()),
          Types.NestedField.required(5, "ps_comment", Types.StringType.get()));

  static {
    // Set HadoopConfig used for the HadoopCatalog.
    CONF.set("fs.azure.account.key.vortexicebergsummit25.dfs.core.windows.net", AZURE_ACCESS_KEY);

    RECORD_COUNTS = Maps.newHashMap();

    RECORD_COUNTS.put("customer_0", 15000000L);
    RECORD_COUNTS.put("lineitem_0", 38510000L);
    RECORD_COUNTS.put("lineitem_1", 38510000L);
    RECORD_COUNTS.put("lineitem_2", 38510000L);
    RECORD_COUNTS.put("lineitem_3", 38510000L);
    RECORD_COUNTS.put("lineitem_4", 38510000L);
    RECORD_COUNTS.put("lineitem_5", 38510000L);
    RECORD_COUNTS.put("lineitem_6", 38510000L);
    RECORD_COUNTS.put("lineitem_7", 38510000L);
    RECORD_COUNTS.put("lineitem_8", 38510000L);
    RECORD_COUNTS.put("lineitem_9", 38510000L);
    RECORD_COUNTS.put("lineitem_10", 38510000L);
    RECORD_COUNTS.put("lineitem_11", 38510000L);
    RECORD_COUNTS.put("lineitem_12", 38510000L);
    RECORD_COUNTS.put("lineitem_13", 38510000L);
    RECORD_COUNTS.put("lineitem_14", 38510000L);
    RECORD_COUNTS.put("lineitem_15", 22387902L);
    RECORD_COUNTS.put("nation_0", 25L);
    RECORD_COUNTS.put("orders_0", 39880000L);
    RECORD_COUNTS.put("orders_1", 39880000L);
    RECORD_COUNTS.put("orders_2", 39880000L);
    RECORD_COUNTS.put("orders_3", 30360000L);
    RECORD_COUNTS.put("part_0", 20000000L);
    RECORD_COUNTS.put("partsupp_0", 29490000L);
    RECORD_COUNTS.put("partsupp_1", 29490000L);
    RECORD_COUNTS.put("partsupp_2", 21020000L);
    RECORD_COUNTS.put("region_0", 5L);
    RECORD_COUNTS.put("supplier_0", 1000000L);
  }

  // Step 1: create the warehouse, populate it with TPCH data.
  @Test
  public void setupWarehouse() throws IOException {
    setupWarehouseFormat(FileFormat.PARQUET);
    setupWarehouseFormat(FileFormat.VORTEX);
  }

  private void setupWarehouseFormat(FileFormat format) throws IOException {
    URI rootUri = URI.create(WAREHOUSE);

    // Create the table
    try (HadoopCatalog catalog = new HadoopCatalog(CONF, WAREHOUSE)) {
      createTable(catalog, rootUri, "customer", CUSTOMER_SCHEMA, 1, format);
      createTable(catalog, rootUri, "lineitem", LINEITEM_SCHEMA, 16, format);
      createTable(catalog, rootUri, "nation", NATION_SCHEMA, 1, format);
      createTable(catalog, rootUri, "orders", ORDERS_SCHEMA, 4, format);
      createTable(catalog, rootUri, "part", PART_SCHEMA, 1, format);
      createTable(catalog, rootUri, "partsupp", PARTSUPP_SCHEMA, 3, format);
      createTable(catalog, rootUri, "region", REGION_SCHEMA, 1, format);
      createTable(catalog, rootUri, "supplier", SUPPLIER_SCHEMA, 1, format);
    }
  }

  private void createTable(
      Catalog catalog, URI rootUri, String name, Schema schema, int count, FileFormat format) {
    TableIdentifier tableId = TableIdentifier.of(format.name().toLowerCase(Locale.ROOT), name);
    Table table = catalog.createTable(tableId, schema);
    List<DataFile> appendFiles = addDataFiles(rootUri, name, count, format);

    // Make a new APPEND transaction.
    AppendFiles txn = table.newAppend();
    appendFiles.forEach(txn::appendFile);
    txn.commit();
  }

  private List<DataFile> addDataFiles(URI rootUri, String name, int count, FileFormat format) {
    List<DataFile> result = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      String baseName = name + "_" + i;
      String fileName = format.addExtension(baseName);
      Path fullPath =
          new Path(new Path(new Path(rootUri), format.name().toLowerCase(Locale.ROOT)), fileName);
      System.err.println("fullPath = " + fullPath);
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withInputFile(HadoopInputFile.fromLocation(fullPath.toString(), CONF))
              .withRecordCount(RECORD_COUNTS.get(baseName))
              .withFormat(format)
              .build();
      result.add(dataFile);
    }

    return result;
  }

  // Run some Spark SQL queries against the warehouse (using partition pruning!)
  @ParameterizedTest
  @ValueSource(strings = {"vortex"})
  public void scanWarehouse(String format) {
    try (SparkSession spark = newSparkSession("scanWarehouse")) {
      spark.sql(String.format("select * from %s.lineitem limit 10", format)).show();
    }
  }

  private static SparkSession newSparkSession(String name) {
    return SparkSession.builder()
        .config(
            "fs.azure.account.key.vortexicebergsummit25.dfs.core.windows.net",
            AZURE_ACCESS_KEY)
        // use Spark iceberg catalog
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        // use hadoop type
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        // set the warehouse path
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
        .config("spark.sql.defaultCatalog", "iceberg")
        .appName(name)
        .master("local")
        .getOrCreate();
  }
}
