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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
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

public class VortexLocalSmokeTest {
  private static final Configuration CONF = new Configuration();
  private static final String WAREHOUSE = "file:///Volumes/Code/tmp/warehouse";

  private static final Map<String, Long> RECORD_COUNTS =
      ImmutableMap.<String, Long>builder()
          .put("employees1.vortex", 3L)
          .put("employees2.vortex", 3L)
          .build();
  private static final Map<String, List<String>> PARTITION_VALUES =
      ImmutableMap.<String, List<String>>builder()
          .put("employees1.vortex", List.of("Company A"))
          .put("employees2.vortex", List.of("Company B"))
          .build();

  private static final Schema EMPLOYEES_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "name", Types.StringType.get()),
          Types.NestedField.required(2, "salary", Types.LongType.get()),
          Types.NestedField.required(16, "employer", Types.StringType.get()));

  // Step 1: create the warehouse, populate it with TPCH data.
  @Test
  public void setupWarehouse() throws IOException {
    setupWarehouseFormat(FileFormat.VORTEX);
  }

  private void setupWarehouseFormat(FileFormat format) throws IOException {
    URI rootUri = URI.create(WAREHOUSE);

    // Create the table
    try (HadoopCatalog catalog = new HadoopCatalog(CONF, WAREHOUSE)) {
      createTable(catalog, "employees", EMPLOYEES_SCHEMA, format);
    }
  }

  private void createTable(Catalog catalog, String name, Schema schema, FileFormat format) {
    TableIdentifier tableId = TableIdentifier.of(format.name().toLowerCase(Locale.ROOT), name);
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("employer").build();
    Table table = catalog.createTable(tableId, schema, spec);
    List<DataFile> appendFiles =
        addDataFiles(
            List.of(
                new Path("file:///Volumes/Code/tmp/warehouse/employees1.vortex"),
                new Path("file:///Volumes/Code/tmp/warehouse/employees2.vortex")),
            spec,
            format);

    // Make a new APPEND transaction.
    AppendFiles txn = table.newAppend();
    appendFiles.forEach(txn::appendFile);
    txn.commit();
  }

  private List<DataFile> addDataFiles(List<Path> dataFiles, PartitionSpec spec, FileFormat format) {
    return dataFiles.stream()
        .map(
            path -> {
              String baseName = path.getName();
              return DataFiles.builder(spec)
                  .withInputFile(HadoopInputFile.fromLocation(path.toString(), CONF))
                  .withRecordCount(RECORD_COUNTS.get(baseName))
                  .withPartitionValues(PARTITION_VALUES.get(baseName))
                  .withFormat(format)
                  .build();
            })
        .collect(Collectors.toList());
  }

  // Run some Spark SQL queries against the warehouse (using partition pruning!)
  @ParameterizedTest
  @ValueSource(strings = {"vortex"})
  public void scanWarehouse(String format) {
    try (SparkSession spark = newSparkSession("scanWarehouse")) {
      spark.read().format("iceberg").load("vortex.employees").registerTempTable("employees");
      spark.sql("SELECT * from employees WHERE employer = 'Company A'").show();
    }
  }

  private static SparkSession newSparkSession(String name) {
    return SparkSession.builder()
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
