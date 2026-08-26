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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestSparkFileTypeScan {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

  private static SparkSession spark = null;

  @TempDir private Path temp;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .master("local[2]")
            .config(TestBase.DISABLE_UI)
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    SparkSession currentSpark = spark;
    spark = null;
    currentSpark.stop();
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "avro"})
  void readsAWholeFileColumn(String format) throws IOException {
    Table table = createTable(format);
    List<Record> expected = records(table.schema());
    Types.FileType fileType = table.schema().findField("photo").type().asFileType();

    List<Row> rows =
        spark
            .read()
            .format("iceberg")
            .load(table.location())
            .select("id", "photo")
            .orderBy("id")
            .collectAsList();

    assertThat(rows).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i += 1) {
      Record expectedPhoto = (Record) expected.get(i).getField("photo");
      Row photo = rows.get(i).getStruct(1);

      assertThat(rows.get(i).getLong(0)).isEqualTo(expected.get(i).getField("id"));
      for (Types.NestedField field : fileType.fields()) {
        assertThat(photo.get(photo.fieldIndex(field.name())))
            .as("Field %s should match", field.name())
            .isEqualTo(sparkValue(expectedPhoto.getField(field.name())));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "avro"})
  void readsASingleNestedFieldOfAFileColumn(String format) throws IOException {
    Table table = createTable(format);
    List<Record> expected = records(table.schema());

    List<Row> rows =
        spark
            .read()
            .format("iceberg")
            .load(table.location())
            .selectExpr("id", "photo.uri AS uri")
            .orderBy("id")
            .collectAsList();

    assertThat(rows).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i += 1) {
      Record expectedPhoto = (Record) expected.get(i).getField("photo");

      assertThat(rows.get(i).getLong(0)).isEqualTo(expected.get(i).getField("id"));
      assertThat(rows.get(i).getString(1)).isEqualTo(expectedPhoto.getField("uri"));
    }
  }

  @Test
  void rejectsWritingAFileColumn() throws IOException {
    Table table = createTable("parquet");
    Dataset<Row> df = spark.read().format("iceberg").load(table.location());

    assertThatThrownBy(() -> df.write().format("iceberg").mode("append").save(table.location()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write file column photo: Spark cannot express the file type");
  }

  private static Object sparkValue(Object value) {
    if (value instanceof ByteBuffer) {
      return ByteBuffers.toByteArray((ByteBuffer) value);
    }

    return value;
  }

  private Table createTable(String format) throws IOException {
    File location = temp.resolve(format).toFile();
    Table table =
        new HadoopTables(new Configuration())
            .create(
                SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.FORMAT_VERSION,
                    "4",
                    TableProperties.DEFAULT_FILE_FORMAT,
                    format),
                location.toURI().toString());

    File dataFolder = new File(location, "data");
    dataFolder.mkdirs();
    File dataFile = new File(dataFolder, FileFormat.fromString(format).addExtension("photos"));
    DataFile file =
        FileHelpers.writeDataFile(table, Files.localOutput(dataFile), records(table.schema()));
    table.newAppend().appendFile(file).commit();

    return table;
  }

  private static List<Record> records(Schema schema) {
    Types.FileType fileType = schema.findField("photo").type().asFileType();
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < 3; i += 1) {
      GenericRecord photo = GenericRecord.create(fileType.asStruct());
      photo.setField("uri", "s3://bucket/photo-" + i + ".png");
      photo.setField("offset", (long) i);
      photo.setField("size", 100L + i);
      photo.setField("content_type", "image/png");
      photo.setField("checksum", "checksum-" + i);
      photo.setField("inline", ByteBuffer.wrap(new byte[] {(byte) i, (byte) (i + 1)}));

      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", (long) i);
      record.setField("photo", photo);
      records.add(record);
    }

    return records;
  }
}
