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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRemoveEmptyFilesProcedure extends SparkExtensionsTestBase {

  private final String sourceTableName = "source_table";
  private File fileTableDir;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public TestRemoveEmptyFilesProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void setupTempDirs() {
    try {
      fileTableDir = temp.newFolder();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s", sourceTableName);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private static final StructField[] struct = {
    new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
    new StructField("name", DataTypes.StringType, true, Metadata.empty()),
    new StructField("dept", DataTypes.StringType, true, Metadata.empty()),
    new StructField("subdept", DataTypes.StringType, true, Metadata.empty())
  };

  private static final Dataset<Row> unpartitionedDF =
      spark
          .createDataFrame(
              ImmutableList.of(
                  RowFactory.create(1, "John Doe", "hr", "communications"),
                  RowFactory.create(2, "Jane Doe", "hr", "salary"),
                  RowFactory.create(3, "Matt Doe", "hr", "communications"),
                  RowFactory.create(4, "Will Doe", "facilities", "all")),
              new StructType(struct))
          .repartition(1);

  private List<String> createUnpartitionedParquetFileTableWithEmptyFiles() {
    String createParquet =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING %s LOCATION '%s'";

    sql(createParquet, sourceTableName, "parquet", fileTableDir.getAbsolutePath());
    unpartitionedDF.write().insertInto(sourceTableName);
    unpartitionedDF.write().insertInto(sourceTableName);
    Set<String> filePaths =
        Arrays.stream(Objects.requireNonNull(fileTableDir.listFiles()))
            .map(File::getAbsolutePath)
            .collect(java.util.stream.Collectors.toSet());
    unpartitionedDF.limit(0).write().insertInto(sourceTableName);
    Set<String> newFilePaths =
        Arrays.stream(Objects.requireNonNull(fileTableDir.listFiles()))
            .map(File::getAbsolutePath)
            .collect(java.util.stream.Collectors.toSet());
    newFilePaths.removeAll(filePaths);
    newFilePaths.removeIf(s -> s.contains("SUCCESS"));
    newFilePaths.removeIf(s -> s.contains("crc"));
    return ImmutableList.copyOf(
        newFilePaths.stream().map(p -> "file:" + p).collect(java.util.stream.Collectors.toList()));
  }

  @Test
  public void testSkipAddingEmptyFile() {
    List<String> emptyFileLocations = createUnpartitionedParquetFileTableWithEmptyFiles();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    sql(
        "CALL %s.system.add_files('%s', '`parquet`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    int fileCount = sql("SELECT file_path FROM %s.files", tableName).size();

    List<Object[]> tableResult =
        sql("CALL %s.system.remove_empty_files(table => '%s')", catalogName, tableName);

    Assert.assertEquals(1, tableResult.size());

    Assert.assertEquals(
        "There should be exactly 1 empty file in the table",
        emptyFileLocations,
        tableResult.stream().map(o -> (String) o[0]).collect(java.util.stream.Collectors.toList()));

    Set<String> dataFiles =
        sql("SELECT file_path FROM %s.files", tableName).stream()
            .map(o -> (String) o[0])
            .collect(Collectors.toSet());
    Assert.assertEquals("Only 1 file is removed", fileCount - 1, dataFiles.size());
    Assert.assertTrue(
        "Data files table should not contain empty file anymore because we perform empty file removal.",
        dataFiles.stream().noneMatch(emptyFileLocations::contains));
  }

  @Test
  public void testSkipAddingEmptyFileWithDryRun() {
    List<String> emptyFileLocations = createUnpartitionedParquetFileTableWithEmptyFiles();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    sql(
        "CALL %s.system.add_files('%s', '`parquet`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    int fileCount = sql("SELECT file_path FROM %s.files", tableName).size();

    List<Object[]> tableResult =
        sql(
            "CALL %s.system.remove_empty_files(table => '%s', dry_run => true)",
            catalogName, tableName);

    Assert.assertEquals(1, tableResult.size());

    Assert.assertEquals(
        "There should be exactly 1 empty file in the table",
        emptyFileLocations,
        tableResult.stream().map(o -> (String) o[0]).collect(java.util.stream.Collectors.toList()));

    Set<String> dataFiles =
        sql("SELECT file_path FROM %s.files", tableName).stream()
            .map(o -> (String) o[0])
            .collect(Collectors.toSet());
    Assert.assertEquals("No file is removed", fileCount, dataFiles.size());
    Assert.assertTrue(
        "Data files table should still contain all empty file because we only run dry_run",
        dataFiles.containsAll(emptyFileLocations));
  }
}
