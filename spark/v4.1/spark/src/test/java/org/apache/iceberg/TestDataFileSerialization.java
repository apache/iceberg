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

import static org.apache.iceberg.TaskCheckHelper.assertEquals;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDataFileSerialization {

  private static final Schema DATE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "date", Types.StringType.get()),
          optional(4, "double", Types.DoubleType.get()));

  private static final PartitionSpec PARTITION_SPEC =
      PartitionSpec.builderFor(DATE_SCHEMA).identity("date").build();

  private static final Map<Integer, Long> VALUE_COUNTS = Maps.newHashMap();
  private static final Map<Integer, Long> NULL_VALUE_COUNTS = Maps.newHashMap();
  private static final Map<Integer, Long> NAN_VALUE_COUNTS = Maps.newHashMap();
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS = Maps.newHashMap();
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS = Maps.newHashMap();

  static {
    VALUE_COUNTS.put(1, 5L);
    VALUE_COUNTS.put(2, 3L);
    VALUE_COUNTS.put(4, 2L);
    NULL_VALUE_COUNTS.put(1, 0L);
    NULL_VALUE_COUNTS.put(2, 2L);
    NAN_VALUE_COUNTS.put(4, 1L);
    LOWER_BOUNDS.put(1, longToBuffer(0L));
    UPPER_BOUNDS.put(1, longToBuffer(4L));
  }

  private static final DataFile DATA_FILE =
      DataFiles.builder(PARTITION_SPEC)
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(1234)
          .withPartitionPath("date=2018-06-08")
          .withMetrics(
              new Metrics(
                  5L,
                  null,
                  VALUE_COUNTS,
                  NULL_VALUE_COUNTS,
                  NAN_VALUE_COUNTS,
                  LOWER_BOUNDS,
                  UPPER_BOUNDS))
          .withSplitOffsets(ImmutableList.of(4L))
          .withEncryptionKeyMetadata(ByteBuffer.allocate(4).putInt(34))
          .withSortOrder(SortOrder.unsorted())
          .build();

  @TempDir private Path temp;

  @Test
  public void testDataFileKryoSerialization() throws Exception {
    File data = File.createTempFile("junit", null, temp.toFile());
    assertThat(data.delete()).isTrue();
    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();

    try (Output out = new Output(new FileOutputStream(data))) {
      kryo.writeClassAndObject(out, DATA_FILE);
      kryo.writeClassAndObject(out, DATA_FILE.copy());
    }

    try (Input in = new Input(new FileInputStream(data))) {
      for (int i = 0; i < 2; i += 1) {
        Object obj = kryo.readClassAndObject(in);
        assertThat(obj).as("Should be a DataFile").isInstanceOf(DataFile.class);
        assertEquals(DATA_FILE, (DataFile) obj);
      }
    }
  }

  @Test
  public void testDataFileJavaSerialization() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(DATA_FILE);
      out.writeObject(DATA_FILE.copy());
    }

    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      for (int i = 0; i < 2; i += 1) {
        Object obj = in.readObject();
        assertThat(obj).as("Should be a DataFile").isInstanceOf(DataFile.class);
        assertEquals(DATA_FILE, (DataFile) obj);
      }
    }
  }

  @Test
  public void testParquetWriterSplitOffsets() throws IOException {
    Iterable<InternalRow> records = RandomData.generateSpark(DATE_SCHEMA, 1, 33L);
    File parquetFile =
        new File(temp.toFile(), FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));
    FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(DATE_SCHEMA)
            .createWriterFunc(
                msgType ->
                    SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(DATE_SCHEMA), msgType))
            .build();
    try {
      writer.addAll(records);
    } finally {
      writer.close();
    }

    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();
    File dataFile = File.createTempFile("junit", null, temp.toFile());
    try (Output out = new Output(new FileOutputStream(dataFile))) {
      kryo.writeClassAndObject(out, writer.splitOffsets());
    }
    try (Input in = new Input(new FileInputStream(dataFile))) {
      kryo.readClassAndObject(in);
    }
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
