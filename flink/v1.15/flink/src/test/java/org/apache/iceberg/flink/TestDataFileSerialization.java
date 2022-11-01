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
package org.apache.iceberg.flink;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestDataFileSerialization {

  private static final Schema DATE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "date", Types.StringType.get()),
          optional(4, "double", Types.DoubleType.get()));

  private static final PartitionSpec PARTITION_SPEC =
      PartitionSpec.builderFor(DATE_SCHEMA).identity("date").build();

  private static final Map<Integer, Long> COLUMN_SIZES = Maps.newHashMap();
  private static final Map<Integer, Long> VALUE_COUNTS = Maps.newHashMap();
  private static final Map<Integer, Long> NULL_VALUE_COUNTS = Maps.newHashMap();
  private static final Map<Integer, Long> NAN_VALUE_COUNTS = Maps.newHashMap();
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS = Maps.newHashMap();
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS = Maps.newHashMap();

  static {
    COLUMN_SIZES.put(1, 2L);
    COLUMN_SIZES.put(2, 3L);
    VALUE_COUNTS.put(1, 5L);
    VALUE_COUNTS.put(2, 3L);
    VALUE_COUNTS.put(4, 2L);
    NULL_VALUE_COUNTS.put(1, 0L);
    NULL_VALUE_COUNTS.put(2, 2L);
    NAN_VALUE_COUNTS.put(4, 1L);
    LOWER_BOUNDS.put(1, longToBuffer(0L));
    UPPER_BOUNDS.put(1, longToBuffer(4L));
  }

  private static final Metrics METRICS =
      new Metrics(
          5L, null, VALUE_COUNTS, NULL_VALUE_COUNTS, NAN_VALUE_COUNTS, LOWER_BOUNDS, UPPER_BOUNDS);

  private static final DataFile DATA_FILE =
      DataFiles.builder(PARTITION_SPEC)
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(1234)
          .withPartitionPath("date=2018-06-08")
          .withMetrics(METRICS)
          .withSplitOffsets(ImmutableList.of(4L))
          .withEncryptionKeyMetadata(ByteBuffer.allocate(4).putInt(34))
          .withSortOrder(SortOrder.unsorted())
          .build();

  private static final DeleteFile POS_DELETE_FILE =
      FileMetadata.deleteFileBuilder(PARTITION_SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/pos-delete.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("date=2018-06-08")
          .withMetrics(METRICS)
          .withEncryptionKeyMetadata(ByteBuffer.allocate(4).putInt(35))
          .withRecordCount(23)
          .build();

  private static final DeleteFile EQ_DELETE_FILE =
      FileMetadata.deleteFileBuilder(PARTITION_SPEC)
          .ofEqualityDeletes(2, 3)
          .withPath("/path/to/equality-delete.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("date=2018-06-08")
          .withMetrics(METRICS)
          .withEncryptionKeyMetadata(ByteBuffer.allocate(4).putInt(35))
          .withRecordCount(23)
          .withSortOrder(SortOrder.unsorted())
          .build();

  @Test
  public void testJavaSerialization() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(DATA_FILE);
      out.writeObject(DATA_FILE.copy());

      out.writeObject(POS_DELETE_FILE);
      out.writeObject(POS_DELETE_FILE.copy());

      out.writeObject(EQ_DELETE_FILE);
      out.writeObject(EQ_DELETE_FILE.copy());
    }

    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      for (int i = 0; i < 2; i += 1) {
        Object obj = in.readObject();
        Assertions.assertThat(obj).as("Should be a DataFile").isInstanceOf(DataFile.class);
        TestHelpers.assertEquals(DATA_FILE, (DataFile) obj);
      }

      for (int i = 0; i < 2; i += 1) {
        Object obj = in.readObject();
        Assertions.assertThat(obj)
            .as("Should be a position DeleteFile")
            .isInstanceOf(DeleteFile.class);
        TestHelpers.assertEquals(POS_DELETE_FILE, (DeleteFile) obj);
      }

      for (int i = 0; i < 2; i += 1) {
        Object obj = in.readObject();
        Assertions.assertThat(obj)
            .as("Should be a equality DeleteFile")
            .isInstanceOf(DeleteFile.class);
        TestHelpers.assertEquals(EQ_DELETE_FILE, (DeleteFile) obj);
      }
    }
  }

  @Test
  public void testDataFileKryoSerialization() throws IOException {
    KryoSerializer<DataFile> kryo = new KryoSerializer<>(DataFile.class, new ExecutionConfig());

    DataOutputSerializer outputView = new DataOutputSerializer(1024);

    kryo.serialize(DATA_FILE, outputView);
    kryo.serialize(DATA_FILE.copy(), outputView);

    DataInputDeserializer inputView = new DataInputDeserializer(outputView.getCopyOfBuffer());
    DataFile dataFile1 = kryo.deserialize(inputView);
    DataFile dataFile2 = kryo.deserialize(inputView);

    TestHelpers.assertEquals(DATA_FILE, dataFile1);
    TestHelpers.assertEquals(DATA_FILE, dataFile2);
  }

  @Test
  public void testDeleteFileKryoSerialization() throws IOException {
    KryoSerializer<DeleteFile> kryo = new KryoSerializer<>(DeleteFile.class, new ExecutionConfig());

    DataOutputSerializer outputView = new DataOutputSerializer(1024);

    kryo.serialize(POS_DELETE_FILE, outputView);
    kryo.serialize(POS_DELETE_FILE.copy(), outputView);

    kryo.serialize(EQ_DELETE_FILE, outputView);
    kryo.serialize(EQ_DELETE_FILE.copy(), outputView);

    DataInputDeserializer inputView = new DataInputDeserializer(outputView.getCopyOfBuffer());

    DeleteFile posDeleteFile1 = kryo.deserialize(inputView);
    DeleteFile posDeleteFile2 = kryo.deserialize(inputView);

    TestHelpers.assertEquals(POS_DELETE_FILE, posDeleteFile1);
    TestHelpers.assertEquals(POS_DELETE_FILE, posDeleteFile2);

    DeleteFile eqDeleteFile1 = kryo.deserialize(inputView);
    DeleteFile eqDeleteFile2 = kryo.deserialize(inputView);

    TestHelpers.assertEquals(EQ_DELETE_FILE, eqDeleteFile1);
    TestHelpers.assertEquals(EQ_DELETE_FILE, eqDeleteFile2);
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
