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
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAddFilesProcedure extends SparkExtensionsTestBase {

  private final String sourceTableName = "source_table";
  private File fileTableDir;

  public TestAddFilesProcedure(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

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

  @Test
  public void addDataUnpartitioned() {
    createUnpartitionedFileTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`orc`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT * FROM %s ORDER BY id", sourceTableName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Ignore  // Classpath issues prevent us from actually writing to a Spark ORC table
  public void addDataUnpartitionedOrc() {
    createUnpartitionedFileTable("orc");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`orc`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT * FROM %s ORDER BY id", sourceTableName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addDataUnpartitionedHive() {
    createUnpartitionedHiveTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '%s')",
        catalogName, tableName, sourceTableName);

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT * FROM %s ORDER BY id", sourceTableName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addDataUnpartitionedExtraCol() {
    createUnpartitionedFileTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String, foo string) USING iceberg";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT * FROM %s ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addDataUnpartitionedMissingCol() {
    createUnpartitionedFileTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String) USING iceberg";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept FROM %s ORDER BY id", sourceTableName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addDataPartitionedMissingCol() {
    createPartitionedFileTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(8L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept FROM %s ORDER BY id", sourceTableName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addDataPartitioned() {
    createPartitionedFileTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(8L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", tableName));
  }

  @Ignore  // Classpath issues prevent us from actually writing to a Spark ORC table
  public void addDataPartitionedOrc() {
    createPartitionedFileTable("orc");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(8L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addDataPartitionedHive() {
    createPartitionedHiveTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '%s')",
        catalogName, tableName, sourceTableName);

    Assert.assertEquals(8L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addPartitionToPartitioned() {
    createPartitionedFileTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`', map('id', 1))",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s WHERE id = 1 ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addFilteredPartitionsToPartitioned() {
    createCompositePartitionedTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg " +
            "PARTITIONED BY (id, dept)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`', map('id', 1))",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s WHERE id = 1 ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addPartitionToPartitionedHive() {
    createPartitionedHiveTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '%s', map('id', 1))",
        catalogName, tableName, sourceTableName);

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s WHERE id = 1 ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", tableName));
  }

  @Test
  public void invalidDataImport() {
    createPartitionedFileTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    AssertHelpers.assertThrows("Should forbid adding of partitioned data to unpartitioned table",
        IllegalArgumentException.class,
        "Cannot use partition filter with an unpartitioned table",
        () -> scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`', map('id', 1))",
            catalogName, tableName, fileTableDir.getAbsolutePath())
    );

    AssertHelpers.assertThrows("Should forbid adding of partitioned data to unpartitioned table",
        IllegalArgumentException.class,
        "Cannot use partition filter with an unpartitioned table",
        () -> scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`')",
            catalogName, tableName, fileTableDir.getAbsolutePath())
    );
  }

  @Test
  public void invalidDataImportPartitioned() {
    createUnpartitionedFileTable("parquet");

    File fileToAdd = fileTableDir.listFiles((dir, name) -> name.endsWith("parquet"))[0];

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    AssertHelpers.assertThrows("Should forbid adding with a mismatching partition spec",
        IllegalArgumentException.class,
        "is greater than the number of partitioned columns",
        () -> scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`', map('x', '1', 'y', '2'))",
            catalogName, tableName, fileTableDir.getAbsolutePath()));

    AssertHelpers.assertThrows("Should forbid adding with partition spec with incorrect columns",
        IllegalArgumentException.class,
        "specified partition filter refers to columns that are not partitioned",
        () -> scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`', map('dept', '2'))",
            catalogName, tableName, fileTableDir.getAbsolutePath()));

  }

  private static final StructField[] struct = {
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("name", DataTypes.StringType, false, Metadata.empty()),
      new StructField("dept", DataTypes.StringType, false, Metadata.empty()),
      new StructField("subdept", DataTypes.StringType, false, Metadata.empty())
  };

  private static final Dataset<Row> unpartitionedDF =
      spark.createDataFrame(
          ImmutableList.of(
              RowFactory.create(1, "John Doe", "hr", "communications"),
              RowFactory.create(2, "Jane Doe", "hr", "salary"),
              RowFactory.create(3, "Matt Doe", "hr", "communications"),
              RowFactory.create(4, "Will Doe", "facilities", "all")),
          new StructType(struct)).repartition(1);

  private static final Dataset<Row> partitionedDF =
      unpartitionedDF.select("name", "dept", "subdept", "id");


  private static final Dataset<Row> compositePartitionedDF =
      unpartitionedDF.select("name", "subdept", "id", "dept");


  private void  createUnpartitionedFileTable(String format) {
    String createParquet =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING %s LOCATION '%s'";

    sql(createParquet, sourceTableName, format, fileTableDir.getAbsolutePath());
    unpartitionedDF.write().insertInto(sourceTableName);
    unpartitionedDF.write().insertInto(sourceTableName);
  }

  private void  createPartitionedFileTable(String format) {
    String createParquet =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING %s PARTITIONED BY (id) " +
            "LOCATION '%s'";

    sql(createParquet, sourceTableName, format, fileTableDir.getAbsolutePath());

    partitionedDF.write().insertInto(sourceTableName);
    partitionedDF.write().insertInto(sourceTableName);
  }

  private void createCompositePartitionedTable(String format) {
    String createParquet = "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING %s " +
        "PARTITIONED BY (id, dept) LOCATION '%s'";
    sql(createParquet, sourceTableName, format, fileTableDir.getAbsolutePath());

    compositePartitionedDF.write().insertInto(sourceTableName);
    compositePartitionedDF.write().insertInto(sourceTableName);
  }

  private void createWeirdCaseTable(String format) {
    String createParquet =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING %s PARTITIONED BY (Dep_OT) " +
            "LOCATION '%s'";

    sql(createParquet, sourceTableName, format, fileTableDir.getAbsolutePath());

    partitionedDF.write().insertInto(sourceTableName);
    partitionedDF.write().insertInto(sourceTableName);

  }

  private void createUnpartitionedHiveTable() {
    String createHive = "CREATE TABLE %s (id Integer, name String, dept String, subdept String) STORED AS parquet";

    sql(createHive, sourceTableName);

    unpartitionedDF.write().insertInto(sourceTableName);
    unpartitionedDF.write().insertInto(sourceTableName);
  }

  private void createPartitionedHiveTable() {
    String createHive = "CREATE TABLE %s (name String, dept String, subdept String) " +
        "PARTITIONED BY (id Integer) STORED AS parquet";

    sql(createHive, sourceTableName);

    partitionedDF.write().insertInto(sourceTableName);
    partitionedDF.write().insertInto(sourceTableName);
  }
}
