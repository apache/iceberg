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
          Types.NestedField.required(5, "l_quantity", Types.DoubleType.get()),
          Types.NestedField.required(6, "l_extendedprice", Types.DoubleType.get()),
          Types.NestedField.required(7, "l_discount", Types.DoubleType.get()),
          Types.NestedField.required(8, "l_tax", Types.DoubleType.get()),
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
          Types.NestedField.required(4, "o_totalprice", Types.DoubleType.get()),
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
          Types.NestedField.required(4, "c_nationkey", Types.IntegerType.get()),
          Types.NestedField.required(5, "c_phone", Types.StringType.get()),
          Types.NestedField.required(6, "c_acctbal", Types.LongType.get()),
          Types.NestedField.required(7, "c_mktsegment", Types.StringType.get()),
          Types.NestedField.required(8, "c_comment", Types.StringType.get()));

  private static final Schema NATION_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "n_nationkey", Types.IntegerType.get()),
          Types.NestedField.required(2, "n_name", Types.StringType.get()),
          Types.NestedField.required(3, "n_regionkey", Types.IntegerType.get()),
          Types.NestedField.optional(4, "n_comment", Types.StringType.get()));

  private static final Schema REGION_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "r_regionkey", Types.IntegerType.get()),
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
          Types.NestedField.required(8, "p_retailprice", Types.DoubleType.get()),
          Types.NestedField.required(9, "p_comment", Types.StringType.get()));

  private static final Schema SUPPLIER_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "s_suppkey", Types.LongType.get()),
          Types.NestedField.required(2, "s_name", Types.StringType.get()),
          Types.NestedField.required(3, "s_address", Types.StringType.get()),
          Types.NestedField.required(4, "s_nationkey", Types.IntegerType.get()),
          Types.NestedField.required(5, "s_phone", Types.StringType.get()),
          Types.NestedField.required(6, "s_acctbal", Types.DoubleType.get()),
          Types.NestedField.required(7, "s_comment", Types.StringType.get()));

  private static final Schema PARTSUPP_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "ps_partkey", Types.LongType.get()),
          Types.NestedField.required(2, "ps_suppkey", Types.LongType.get()),
          Types.NestedField.required(3, "ps_availqty", Types.LongType.get()),
          Types.NestedField.required(4, "ps_supplycost", Types.DoubleType.get()),
          Types.NestedField.required(5, "ps_comment", Types.StringType.get()));

  static {
    // Set HadoopConfig used for the HadoopCatalog.
    CONF.set("fs.azure.account.key.vortexicebergsummit25.dfs.core.windows.net", AZURE_ACCESS_KEY);

    RECORD_COUNTS = Maps.newHashMap();

    RECORD_COUNTS.put("customer_0", 15000000L);
    RECORD_COUNTS.put("lineitem_0", 35300000L);
    RECORD_COUNTS.put("lineitem_1", 35300000L);
    RECORD_COUNTS.put("lineitem_2", 35300000L);
    RECORD_COUNTS.put("lineitem_3", 35300000L);
    RECORD_COUNTS.put("lineitem_4", 35300000L);
    RECORD_COUNTS.put("lineitem_5", 35300000L);
    RECORD_COUNTS.put("lineitem_6", 35300000L);
    RECORD_COUNTS.put("lineitem_7", 35300000L);
    RECORD_COUNTS.put("lineitem_8", 35300000L);
    RECORD_COUNTS.put("lineitem_9", 35300000L);
    RECORD_COUNTS.put("lineitem_10", 35300000L);
    RECORD_COUNTS.put("lineitem_11", 35300000L);
    RECORD_COUNTS.put("lineitem_12", 35300000L);
    RECORD_COUNTS.put("lineitem_13", 35300000L);
    RECORD_COUNTS.put("lineitem_14", 35300000L);
    RECORD_COUNTS.put("lineitem_15", 35300000L);
    RECORD_COUNTS.put("lineitem_16", 35237902L);
    RECORD_COUNTS.put("nation_0", 25L);
    RECORD_COUNTS.put("orders_0", 38140000L);
    RECORD_COUNTS.put("orders_1", 38140000L);
    RECORD_COUNTS.put("orders_2", 38140000L);
    RECORD_COUNTS.put("orders_3", 35580000L);
    RECORD_COUNTS.put("part_0", 20000000L);
    RECORD_COUNTS.put("partsupp_0", 28030000L);
    RECORD_COUNTS.put("partsupp_1", 28030000L);
    RECORD_COUNTS.put("partsupp_2", 23940000L);
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
      createTable(catalog, rootUri, "lineitem", LINEITEM_SCHEMA, 17, format);
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
      spark
          .read()
          .format("iceberg")
          .option("split-size", 1_000_000)
          .load("vortex.customer")
          .registerTempTable("customer");
      spark
          .read()
          .format("iceberg")
          .option("split-size", 3_000_000)
          .load("vortex.lineitem")
          .registerTempTable("lineitem");
      spark
          .read()
          .format("iceberg")
          .option("split-size", 25)
          .load("vortex.nation")
          .registerTempTable("nation");
      spark
          .read()
          .format("iceberg")
          .option("split-size", 1_000_000)
          .load("vortex.orders")
          .registerTempTable("orders");
      spark
          .read()
          .format("iceberg")
          .option("split-size", 1_000_000)
          .load("vortex.partsupp")
          .registerTempTable("partsupp");
      spark
          .read()
          .format("iceberg")
          .option("split-size", 1_000_000)
          .load("vortex.part")
          .registerTempTable("part");
      spark
          .read()
          .format("iceberg")
          .option("split-size", 1_000_000)
          .load("vortex.region")
          .registerTempTable("region");
      spark
          .read()
          .format("iceberg")
          .option("split-size", 1_000_000)
          .load("vortex.supplier")
          .registerTempTable("supplier");

      System.err.println("COUNT: " + spark.sql(Q10).count());
    }
  }

  private static SparkSession newSparkSession(String name) {
    return SparkSession.builder()
        .config("fs.azure.account.key.vortexicebergsummit25.dfs.core.windows.net", AZURE_ACCESS_KEY)
        // use Spark iceberg catalog
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

  static final String Q16 =
      "select\n"
          + "    p_brand,\n"
          + "    p_type,\n"
          + "    p_size,\n"
          + "    count(distinct ps_suppkey) as supplier_cnt\n"
          + "from\n"
          + "    partsupp,\n"
          + "    part\n"
          + "where\n"
          + "        p_partkey = ps_partkey\n"
          + "  and p_brand <> 'Brand#45'\n"
          + "  and p_type not like 'MEDIUM POLISHED%'\n"
          + "  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)\n"
          + "  and ps_suppkey not in (\n"
          + "    select\n"
          + "        s_suppkey\n"
          + "    from\n"
          + "        supplier\n"
          + "    where\n"
          + "            s_comment like '%Customer%Complaints%'\n"
          + ")\n"
          + "group by\n"
          + "    p_brand,\n"
          + "    p_type,\n"
          + "    p_size\n"
          + "order by\n"
          + "    supplier_cnt desc,\n"
          + "    p_brand,\n"
          + "    p_type,\n"
          + "    p_size;";

  static final String Q5 =
      "select\n"
          + "    n_name,\n"
          + "    sum(l_extendedprice * (1 - l_discount)) as revenue\n"
          + "from\n"
          + "    customer,\n"
          + "    orders,\n"
          + "    lineitem,\n"
          + "    supplier,\n"
          + "    nation,\n"
          + "    region\n"
          + "where\n"
          + "        c_custkey = o_custkey\n"
          + "  and l_orderkey = o_orderkey\n"
          + "  and l_suppkey = s_suppkey\n"
          + "  and c_nationkey = s_nationkey\n"
          + "  and s_nationkey = n_nationkey\n"
          + "  and n_regionkey = r_regionkey\n"
          + "  and r_name = 'ASIA'\n"
          + "  and o_orderdate >= date '1994-01-01'\n"
          + "  and o_orderdate < date '1995-01-01'\n"
          + "group by\n"
          + "    n_name\n"
          + "order by\n"
          + "    revenue desc;";

  static final String Q10 =
      "select\n"
          + "    c_custkey,\n"
          + "    c_name,\n"
          + "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n"
          + "    c_acctbal,\n"
          + "    n_name,\n"
          + "    c_address,\n"
          + "    c_phone,\n"
          + "    c_comment\n"
          + "from\n"
          + "    customer,\n"
          + "    orders,\n"
          + "    lineitem,\n"
          + "    nation\n"
          + "where\n"
          + "        c_custkey = o_custkey\n"
          + "  and l_orderkey = o_orderkey\n"
          + "  and o_orderdate >= date '1993-10-01'\n"
          + "  and o_orderdate < date '1994-01-01'\n"
          + "  and l_returnflag = 'R'\n"
          + "  and c_nationkey = n_nationkey\n"
          + "group by\n"
          + "    c_custkey,\n"
          + "    c_name,\n"
          + "    c_acctbal,\n"
          + "    c_phone,\n"
          + "    n_name,\n"
          + "    c_address,\n"
          + "    c_comment\n"
          + "order by\n"
          + "    revenue desc;";
}
