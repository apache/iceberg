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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.RemoveIds;
import org.apache.iceberg.hive.HiveTableBaseTest;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestNameMappingProjection extends HiveTableBaseTest {
  private static final Configuration CONF = HiveTableBaseTest.hiveConf;
  private static SparkSession spark = null;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startSpark() {
    String metastoreURI = CONF.get(HiveConf.ConfVars.METASTOREURIS.varname);

    // Create a spark session.
    TestNameMappingProjection.spark =
        SparkSession.builder()
            .master("local[2]")
            .enableHiveSupport()
            .config("spark.hadoop.hive.metastore.uris", metastoreURI)
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestNameMappingProjection.spark;
    // Stop the spark session.
    TestNameMappingProjection.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testOrcReaderWithNameMapping() throws IOException {
    File orcFile = temp.newFolder();
    TypeDescription orcSchema = TypeDescription.createStruct();
    orcSchema.addField("id", TypeDescription.createInt());
    orcSchema.addField("name", TypeDescription.createString());

    Path dataFilePath = new Path(orcFile.toString(), "name-mapping-data.orc");
    try (org.apache.orc.Writer writer =
        OrcFile.createWriter(
            dataFilePath, OrcFile.writerOptions(new Configuration()).setSchema(orcSchema))) {
      VectorizedRowBatch batch = orcSchema.createRowBatch();
      byte[] aliceVal = "Alice".getBytes(StandardCharsets.UTF_8);
      byte[] bobVal = "Bob".getBytes(StandardCharsets.UTF_8);

      int rowId = batch.size++;
      batch.cols[0].isNull[rowId] = false;
      ((LongColumnVector) batch.cols[0]).vector[rowId] = 1;
      batch.cols[1].isNull[rowId] = false;
      ((BytesColumnVector) batch.cols[1]).setRef(rowId, bobVal, 0, bobVal.length);

      rowId = batch.size++;
      batch.cols[0].isNull[rowId] = false;
      ((LongColumnVector) batch.cols[0]).vector[rowId] = 2;
      batch.cols[1].isNull[rowId] = false;
      ((BytesColumnVector) batch.cols[1]).setRef(rowId, aliceVal, 0, aliceVal.length);

      writer.addRowBatch(batch);
      batch.reset();
    }

    File fileWithData = new File(dataFilePath.toString());
    DataFile orcDataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFormat("orc")
            .withFileSizeInBytes(fileWithData.length())
            .withPath(fileWithData.getAbsolutePath())
            .withRecordCount(2)
            .build();

    assertNameMappingProjection(orcDataFile, "orc_table");
  }

  @Test
  public void testAvroReaderWithNameMapping() throws IOException {
    File avroFile = temp.newFile();
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .namespace("org.apache.iceberg.spark.data")
            .fields()
            .requiredInt("id")
            .requiredString("name")
            .endRecord();

    org.apache.avro.Schema avroSchemaWithoutIds = RemoveIds.removeIds(avroSchema);

    GenericRecord record1 = new GenericData.Record(avroSchemaWithoutIds);
    record1.put("id", 1);
    record1.put("name", "Bob");

    GenericRecord record2 = new GenericData.Record(avroSchemaWithoutIds);
    record2.put("id", 2);
    record2.put("name", "Alice");

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchemaWithoutIds);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

    dataFileWriter.create(avroSchemaWithoutIds, avroFile);
    dataFileWriter.append(record1);
    dataFileWriter.append(record2);
    dataFileWriter.close();

    DataFile avroDataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFormat("avro")
            .withFileSizeInBytes(avroFile.length())
            .withPath(avroFile.getAbsolutePath())
            .withRecordCount(2)
            .build();

    assertNameMappingProjection(avroDataFile, "avro_table");
  }

  private void assertNameMappingProjection(DataFile dataFile, String tableName) {
    Schema filteredSchema = new Schema(required(1, "name", Types.StringType.get()));
    NameMapping nameMapping = MappingUtil.create(filteredSchema);

    Schema tableSchema =
        new Schema(
            required(1, "name", Types.StringType.get()),
            optional(2, "id", Types.IntegerType.get()));

    Table table =
        catalog.createTable(
            org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, tableName),
            tableSchema,
            PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping))
        .commit();

    table.newFastAppend().appendFile(dataFile).commit();

    List<Row> actual =
        spark
            .read()
            .format("iceberg")
            .load(String.format("%s.%s", DB_NAME, tableName))
            .filter("name='Alice'")
            .collectAsList();

    Assert.assertEquals("Should project 1 record", 1, actual.size());
    Assert.assertEquals("Should equal to 'Alice'", "Alice", actual.get(0).getString(0));
    Assert.assertNull("should be null", actual.get(0).get(1));
  }
}
