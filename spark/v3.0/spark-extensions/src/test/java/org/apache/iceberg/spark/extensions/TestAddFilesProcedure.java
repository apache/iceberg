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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

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

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`')",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(2L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT * FROM %s ORDER BY id", sourceTableName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Ignore // TODO Classpath issues prevent us from actually writing to a Spark ORC table
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
  public void addAvroFile() throws Exception {
    // Spark Session Catalog cannot load metadata tables
    // with "The namespace in session catalog must have exactly one name part"
    Assume.assumeFalse(catalogName.equals("spark_catalog"));

    // Create an Avro file

    Schema schema = SchemaBuilder.record("record").fields()
        .requiredInt("id")
        .requiredString("data")
        .endRecord();
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", 1L);
    record1.put("data", "a");
    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", 2L);
    record2.put("data", "b");
    File outputFile = temp.newFile("test.avro");

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter(datumWriter);
    dataFileWriter.create(schema, outputFile);
    dataFileWriter.append(record1);
    dataFileWriter.append(record2);
    dataFileWriter.close();

    String createIceberg =
        "CREATE TABLE %s (id Long, data String) USING iceberg";
    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`avro`.`%s`')",
        catalogName, tableName, outputFile.getPath());
    Assert.assertEquals(1L, result);

    List<Object[]> expected = Lists.newArrayList(
        new Object[]{1L, "a"},
        new Object[]{2L, "b"}
    );

    assertEquals("Iceberg table contains correct data",
        expected,
        sql("SELECT * FROM %s ORDER BY id", tableName));

    List<Object[]> actualRecordCount = sql("select %s from %s.files",
        DataFile.RECORD_COUNT.name(),
        tableName);
    List<Object[]> expectedRecordCount = Lists.newArrayList();
    expectedRecordCount.add(new Object[]{2L});
    assertEquals("Iceberg file metadata should have correct metadata count",
        expectedRecordCount, actualRecordCount);
  }

  // TODO Adding spark-avro doesn't work in tests
  @Test
  public void addDataUnpartitionedAvro() {
    createUnpartitionedFileTable("avro");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`avro`.`%s`')",
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

  @Ignore  // TODO Classpath issues prevent us from actually writing to a Spark ORC table
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

  // TODO Adding spark-avro doesn't work in tests
  @Ignore
  public void addDataPartitionedAvro() {
    createPartitionedFileTable("avro");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`avro`.`%s`')",
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
  public void addFilteredPartitionsToPartitioned2() {
    createCompositePartitionedTable("parquet");

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg " +
            "PARTITIONED BY (id, dept)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`', map('dept', 'hr'))",
        catalogName, tableName, fileTableDir.getAbsolutePath());

    Assert.assertEquals(6L, result);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s WHERE dept = 'hr' ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s ORDER BY id", tableName));
  }

  @Test
  public void addWeirdCaseHiveTable() {
    createWeirdCaseTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, `naMe` String, dept String, subdept String) USING iceberg " +
            "PARTITIONED BY (`naMe`)";

    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '%s', map('naMe', 'John Doe'))",
        catalogName, tableName, sourceTableName);

    Assert.assertEquals(2L, result);

    /*
    While we would like to use
    SELECT id, `naMe`, dept, subdept FROM %s WHERE `naMe` = 'John Doe' ORDER BY id
    Spark does not actually handle this pushdown correctly for hive based tables and it returns 0 records
     */
    List<Object[]> expected =
        sql("SELECT id, `naMe`, dept, subdept from %s ORDER BY id", sourceTableName)
            .stream()
            .filter(r -> r[1].equals("John Doe"))
            .collect(Collectors.toList());

    // TODO when this assert breaks Spark fixed the pushdown issue
    Assert.assertEquals("If this assert breaks it means that Spark has fixed the pushdown issue", 0,
        sql("SELECT id, `naMe`, dept, subdept from %s WHERE `naMe` = 'John Doe' ORDER BY id", sourceTableName)
            .size());

    // Pushdown works for iceberg
    Assert.assertEquals("We should be able to pushdown mixed case partition keys", 2,
        sql("SELECT id, `naMe`, dept, subdept FROM %s WHERE `naMe` = 'John Doe' ORDER BY id", tableName)
            .size());

    assertEquals("Iceberg table contains correct data",
        expected,
        sql("SELECT id, `naMe`, dept, subdept FROM %s ORDER BY id", tableName));
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
        "Cannot add partitioned files to an unpartitioned table",
        () -> scalarSql("CALL %s.system.add_files('%s', '`parquet`.`%s`')",
            catalogName, tableName, fileTableDir.getAbsolutePath())
    );
  }

  @Test
  public void invalidDataImportPartitioned() {
    createUnpartitionedFileTable("parquet");

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


  @Test
  public void addTwice() {
    createPartitionedHiveTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result1 = scalarSql("CALL %s.system.add_files(" +
            "table => '%s', " +
            "source_table => '%s', " +
            "partition_filter => map('id', 1))",
        catalogName, tableName, sourceTableName);
    Assert.assertEquals(2L, result1);

    Object result2 = scalarSql("CALL %s.system.add_files(" +
            "table => '%s', " +
            "source_table => '%s', " +
            "partition_filter => map('id', 2))",
        catalogName, tableName, sourceTableName);
    Assert.assertEquals(2L, result2);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s WHERE id = 1 ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s WHERE id = 1 ORDER BY id", tableName));
    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s WHERE id = 2 ORDER BY id", sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s WHERE id = 2 ORDER BY id", tableName));
  }

  @Test
  public void duplicateDataPartitioned() {
    createPartitionedHiveTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    scalarSql("CALL %s.system.add_files(" +
            "table => '%s', " +
            "source_table => '%s', " +
            "partition_filter => map('id', 1))",
        catalogName, tableName, sourceTableName);

    AssertHelpers.assertThrows("Should not allow adding duplicate files",
        IllegalStateException.class,
        "Cannot complete import because data files to be imported already" +
            " exist within the target table",
        () -> scalarSql("CALL %s.system.add_files(" +
            "table => '%s', " +
            "source_table => '%s', " +
            "partition_filter => map('id', 1))",
        catalogName, tableName, sourceTableName));
  }

  @Test
  public void duplicateDataPartitionedAllowed() {
    createPartitionedHiveTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg PARTITIONED BY (id)";

    sql(createIceberg, tableName);

    Object result1 = scalarSql("CALL %s.system.add_files(" +
            "table => '%s', " +
            "source_table => '%s', " +
            "partition_filter => map('id', 1))",
        catalogName, tableName, sourceTableName);

    Assert.assertEquals(2L, result1);

    Object result2 = scalarSql("CALL %s.system.add_files(" +
            "table => '%s', " +
            "source_table => '%s', " +
            "partition_filter => map('id', 1)," +
            "check_duplicate_files => false)",
        catalogName, tableName, sourceTableName);

    Assert.assertEquals(2L, result2);


    assertEquals("Iceberg table contains correct data",
        sql("SELECT id, name, dept, subdept FROM %s WHERE id = 1 UNION ALL " +
            "SELECT id, name, dept, subdept FROM %s WHERE id = 1", sourceTableName, sourceTableName),
        sql("SELECT id, name, dept, subdept FROM %s", tableName, tableName));
  }

  @Test
  public void duplicateDataUnpartitioned() {
    createUnpartitionedHiveTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    scalarSql("CALL %s.system.add_files('%s', '%s')",
        catalogName, tableName, sourceTableName);

    AssertHelpers.assertThrows("Should not allow adding duplicate files",
        IllegalStateException.class,
        "Cannot complete import because data files to be imported already" +
            " exist within the target table",
        () -> scalarSql("CALL %s.system.add_files('%s', '%s')",
            catalogName, tableName, sourceTableName));
  }

  @Test
  public void duplicateDataUnpartitionedAllowed() {
    createUnpartitionedHiveTable();

    String createIceberg =
        "CREATE TABLE %s (id Integer, name String, dept String, subdept String) USING iceberg";

    sql(createIceberg, tableName);

    Object result1 = scalarSql("CALL %s.system.add_files('%s', '%s')",
        catalogName, tableName, sourceTableName);
    Assert.assertEquals(2L, result1);

    Object result2 = scalarSql("CALL %s.system.add_files(" +
        "table => '%s', " +
            "source_table => '%s'," +
            "check_duplicate_files => false)",
        catalogName, tableName, sourceTableName);
    Assert.assertEquals(2L, result2);

    assertEquals("Iceberg table contains correct data",
        sql("SELECT * FROM (SELECT * FROM %s UNION ALL " +
            "SELECT * from %s) ORDER BY id", sourceTableName, sourceTableName),
        sql("SELECT * FROM %s ORDER BY id", tableName));


  }


  @Test
  public void addOrcFileWithDoubleAndFloatColumns() throws Exception {
    // Spark Session Catalog cannot load metadata tables
    // with "The namespace in session catalog must have exactly one name part"
    Assume.assumeFalse(catalogName.equals("spark_catalog"));

    // Create an ORC file
    File outputFile = temp.newFile("test.orc");
    List<Record> expectedRecords = createOrcInputFile(outputFile);
    String createIceberg =
        "CREATE TABLE %s (x float, y double, z long) USING iceberg";
    sql(createIceberg, tableName);

    Object result = scalarSql("CALL %s.system.add_files('%s', '`orc`.`%s`')",
        catalogName, tableName, outputFile.getPath());
    Assert.assertEquals(1L, result);

    List<Object[]> expected = Lists.newArrayList(
        new Object[]{1L, "a"},
        new Object[]{2L, "b"}
    );

    assertEquals("Iceberg table contains correct data",
        expected,
        sql("SELECT * FROM %s ORDER BY id", tableName));

    List<Object[]> actualRecordCount = sql("select %s from %s.files",
        DataFile.RECORD_COUNT.name(),
        tableName);
    List<Object[]> expectedRecordCount = Lists.newArrayList();
    expectedRecordCount.add(new Object[]{2L});
    assertEquals("Iceberg file metadata should have correct metadata count",
        expectedRecordCount, actualRecordCount);
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

  private static final Dataset<Row> weirdColumnNamesDF =
      unpartitionedDF.select(
          unpartitionedDF.col("id"),
          unpartitionedDF.col("subdept"),
          unpartitionedDF.col("dept"),
          unpartitionedDF.col("name").as("naMe"));


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

  private void createWeirdCaseTable() {
    String createParquet =
        "CREATE TABLE %s (id Integer, subdept String, dept String) " +
            "PARTITIONED BY (`naMe` String) STORED AS parquet";

    sql(createParquet, sourceTableName);

    weirdColumnNamesDF.write().insertInto(sourceTableName);
    weirdColumnNamesDF.write().insertInto(sourceTableName);

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

  public List<Record> createOrcInputFile(File orcFile) throws IOException {
    if (orcFile.exists()) {
      orcFile.delete();
    }
    // final org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
    //     optional(1, "_boolean", Types.BooleanType.get()),
    //     optional(2, "_int", Types.IntegerType.get()),
    //     optional(3, "_long", Types.LongType.get()),
    //     optional(4, "_float", Types.FloatType.get()),
    //     optional(5, "_double", Types.DoubleType.get()),
    //     optional(6, "_date", Types.DateType.get()),
    //     optional(7, "_time", Types.TimeType.get()),
    //     optional(8, "_timestamp", Types.TimestampType.withoutZone()),
    //     optional(9, "_timestamptz", Types.TimestampType.withZone()),
    //     optional(10, "_string", Types.StringType.get()),
    //     optional(11, "_uuid", Types.UUIDType.get()),
    //     optional(12, "_fixed", Types.FixedType.ofLength(4)),
    //     optional(13, "_binary", Types.BinaryType.get()),
    //     optional(14, "_int_decimal", Types.DecimalType.of(8, 2)),
    //     optional(15, "_long_decimal", Types.DecimalType.of(14, 2)),
    //     optional(16, "_fixed_decimal", Types.DecimalType.of(31, 2))
    // );
    final org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
        optional(1, "x", Types.FloatType.get()),
        optional(2, "y", Types.DoubleType.get()),
        optional(3, "z", Types.LongType.get())
    );

    // final LocalDate date =  LocalDate.parse("2018-06-29", DateTimeFormatter.ISO_LOCAL_DATE);
    // final LocalTime time = LocalTime.parse("10:02:34.000000", DateTimeFormatter.ISO_LOCAL_TIME);
    // final OffsetDateTime timestamptz = OffsetDateTime.parse("2018-06-29T10:02:34.000000+00:00",
    //     DateTimeFormatter.ISO_DATE_TIME);
    // final LocalDateTime timestamp = LocalDateTime.parse("2018-06-29T10:02:34.000000",
    //     DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    // final byte[] fixed = "abcd".getBytes(StandardCharsets.UTF_8);

    List<Record> records = new ArrayList<>();
    // create 50 records
    for (int i = 0; i < 5; i += 1) {
      Record record = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
      record.setField("x",((float) (100 - i)) / 100F + 1.0F); // 2.0f, 1.99f, 1.98f, ...
      record.setField("y",((double) i) / 100.0D + 2.0D); // 2.0d, 2.01d, 2.02d, ...
      record.setField("z", 5_000_000_000L + i);
      // record.setField("_boolean", false);
      // record.setField("_int", i);
      // record.setField("_long", 5_000_000_000L + i);
      // record.setField("_float", ((float) (100 - i)) / 100F + 1.0F); // 2.0f, 1.99f, 1.98f, ...
      // record.setField("_double", ((double) i) / 100.0D + 2.0D); // 2.0d, 2.01d, 2.02d, ...
      // record.setField("_date", date);
      // record.setField("_time", time);
      // record.setField("_timestamp", timestamp);
      // record.setField("_timestamptz", timestamptz);
      // record.setField("_string", "tapir");
      // record.setField("_fixed", fixed);
      // record.setField("_binary", ByteBuffer.wrap("xyz".getBytes(StandardCharsets.UTF_8)));
      // record.setField("_int_decimal", new BigDecimal("77.77"));
      // record.setField("_long_decimal", new BigDecimal("88.88"));
      // record.setField("_fixed_decimal", new BigDecimal("99.99"));
      records.add(record);
    }

    OutputFile outFile = Files.localOutput(orcFile);
    try (FileAppender<Record> appender = org.apache.iceberg.orc.ORC.write(outFile)
        .schema(icebergSchema)
        .metricsConfig(MetricsConfig.fromProperties(ImmutableMap.of("write.metadata.metrics.default", "none")))
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      appender.addAll(records);
    }

    return records;
  }

  private org.apache.hadoop.fs.Path createOrcFile(File orcFileDir, String fileName) throws IOException {
    Configuration conf = new Configuration();
    String schemaFromDemo = "struct<x:int,y:int>";
    String schemaDesired = "struct<x:float,y:double,z:long>"; // TODO - Maybe remove the long
    TypeDescription schema = TypeDescription.fromString(schemaFromDemo);
    org.apache.hadoop.fs.Path orcFile = new org.apache.hadoop.fs.Path(orcFileDir.getAbsolutePath(), fileName);
    Writer writer = OrcFile.createWriter(
        orcFile,
        OrcFile.writerOptions(conf)
            .setSchema(schema));

    int numRows = 4;
    VectorizedRowBatch batch = schema.createRowBatch();
    // Orc implements both Float and Double as DoubleColumnVector
    DoubleColumnVector x = (DoubleColumnVector) batch.cols[0];
    DoubleColumnVector y = (DoubleColumnVector) batch.cols[1];
    LongColumnVector z = (LongColumnVector) batch.cols[2];
    for (int r = 0; r < numRows; ++r) {
      int row = batch.size++;
      x.vector[row] = r;
      y.vector[row] = r * 2;
      z.vector[row] = r + 3;
      // If the batch is full, write it out and start over.
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();

    return orcFile;
  }
}
