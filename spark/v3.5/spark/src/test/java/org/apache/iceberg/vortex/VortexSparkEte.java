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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public final class VortexSparkEte {
  private static final Schema EMPLOYEE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "name", Types.StringType.get()),
          required(3, "salary", Types.LongType.get()));

  private static final Schema LINEITEM_SCHEMA =
      new Schema(
          required(1, "l_orderkey", Types.LongType.get()),
          required(2, "l_partkey", Types.LongType.get()),
          required(3, "l_suppkey", Types.LongType.get()),
          required(4, "l_linenumber", Types.LongType.get()),
          required(5, "l_quantity", Types.DoubleType.get()),
          required(6, "l_extendedprice", Types.DoubleType.get()),
          required(7, "l_discount", Types.DoubleType.get()),
          required(8, "l_tax", Types.DoubleType.get()),
          required(9, "l_returnflag", Types.StringType.get()),
          required(10, "l_linestatus", Types.StringType.get()),
          required(11, "l_shipdate", Types.DateType.get()),
          required(12, "l_commitdate", Types.DateType.get()),
          required(13, "l_receiptdate", Types.DateType.get()),
          required(14, "l_shipinstruct", Types.StringType.get()),
          required(15, "l_shipmode", Types.StringType.get()),
          required(16, "l_comment", Types.StringType.get()));

  private static final Path LINEITEM_PATH =
      Paths.get("/Users/aduffy/code/vortex/bench-vortex/data/tpch/1/lineitem.vortex");

  private static Table employeeTable;
  private static Table lineitemTable;

  @TempDir private static File tempDir;

  @BeforeAll
  public static void beforeAll() {
    Tables tables = new HadoopTables();
    employeeTable =
        createTable(tables, "employees", new ResourceFile("employees.vortex"), EMPLOYEE_SCHEMA, 3);
    lineitemTable =
        createTable(tables, "lineitem", new DiskFile(LINEITEM_PATH), LINEITEM_SCHEMA, 6_001_215);
  }

  private static Table createTable(
      Tables tables, String name, SourceFile sourceFile, Schema schema, int rowCount) {
    File tableDir = new File(tempDir, name);
    String tableLocation = tableDir.getAbsolutePath();
    Table theTable = tables.create(schema, tableLocation);

    // Import the file from the classpath into the table's data directory.
    File dataDir = new File(tableDir, "data");
    dataDir.mkdirs();

    Path newFilePath = new File(dataDir, "1.vortex").toPath();
    sourceFile.writeToPath(newFilePath);

    // Append the data file to this table
    try (FileIO io = theTable.io()) {
      InputFile inputFile = io.newInputFile(newFilePath.toAbsolutePath().toString());
      DataFile newDataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withInputFile(inputFile)
              .withRecordCount(rowCount)
              .build();

      AppendFiles append = theTable.newAppend();
      append.appendFile(newDataFile);
      append.commit();

      assertThat(theTable.currentSnapshot().addedDataFiles(io)).hasSize(1);
      assertThat(
              Iterables.getOnlyElement(theTable.currentSnapshot().addedDataFiles(io)).recordCount())
          .isEqualTo(rowCount);
    }

    return theTable;
  }

  @AfterAll
  public static void afterAll() {
    // Delete the table and all of its data/metadata.
    if (employeeTable != null) {
      new HadoopTables().dropTable(employeeTable.location());
    }

    if (lineitemTable != null) {
      new HadoopTables().dropTable(lineitemTable.location());
    }
  }

  @Test
  public void testBasic() {
    SparkSession spark = SparkSession.builder().master("local").appName("testBasic").getOrCreate();

    System.out.println("LOADING FROM TABLE: " + employeeTable.location());
    Dataset<Row> employees = spark.read().format("iceberg").load(employeeTable.location());
    assertThat(employees.count()).isEqualTo(3);

    employees.printSchema();
    // Show all columns
    employees.show();
    // Show with projection
    employees.select("name").show();
    // Show with filter
    assertThat(employees.where("id > 1").count()).isEqualTo(2);
  }

  @Test
  public void testTPCH() {
    SparkSession spark = SparkSession.builder().master("local").appName("testTPCH").getOrCreate();

    System.out.println("LOADING FROM TABLE: " + lineitemTable.location());
    Dataset<Row> lineitems = spark.read().format("iceberg").load(lineitemTable.location());
    assertThat(lineitems.count()).isEqualTo(6_001_215);

    lineitems.printSchema();
    // Show all columns
    lineitems.show();
    // Show with projection
    lineitems.select("l_shipdate").limit(10).show();
  }

  @Test
  public void testS3() {
    try (SparkSession spark =
        SparkSession.builder()
            .master("local")
            .appName("testS3")
            .config("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY"))
            .config("fs.s3a.secret.key", System.getenv("AWS_SECRET_KEY"))
            .getOrCreate()) {

      // Open a file against the remote
      Dataset<Row> df =
          spark
              .read()
              .format("parquet")
              .load("s3a://vortex-iceberg-dev/iceberg-parquet/customer.parquet");
      df.show();
      df.printSchema();
    }
  }

  interface SourceFile {
    void writeToPath(Path path);
  }

  static class ResourceFile implements SourceFile {
    private final String resourcePath;

    ResourceFile(String resourcePath) {
      this.resourcePath = resourcePath;
    }

    @Override
    public void writeToPath(Path path) {
      try (InputStream inputStream = VortexSparkEte.class.getResourceAsStream(resourcePath);
          OutputStream outputStream = Files.newOutputStream(path)) {
        Preconditions.checkNotNull(inputStream, "Cannot find resource: " + resourcePath);
        ByteStreams.copy(inputStream, outputStream);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load resource: " + resourcePath, e);
      }
    }
  }

  static class DiskFile implements SourceFile {
    private final Path sourcePath;

    DiskFile(Path sourcePath) {
      this.sourcePath = sourcePath;
    }

    @Override
    public void writeToPath(Path path) {
      try (InputStream inputStream = Files.newInputStream(sourcePath);
          OutputStream outputStream = Files.newOutputStream(path)) {
        ByteStreams.copy(inputStream, outputStream);
      } catch (IOException e) {
        throw new RuntimeException("Failed to copy file: " + sourcePath, e);
      }
    }
  }
}
