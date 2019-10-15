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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestKryoSerialization {

  private static final Schema DATE_SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "date", Types.StringType.get()));

  private static final PartitionSpec PARTITION_SPEC = PartitionSpec
      .builderFor(DATE_SCHEMA)
      .identity("date")
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testDataFileKryoSerialization() throws Exception {
    File data = temp.newFile();
    Assert.assertTrue(data.delete());
    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();

    Map<Integer, Long> valueCounts = Maps.newHashMap();
    valueCounts.put(1, 5L);
    valueCounts.put(2, 3L);

    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    nullValueCounts.put(1, 0L);
    nullValueCounts.put(2, 2L);

    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    lowerBounds.put(1, longToBuffer(0L));

    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    upperBounds.put(1, longToBuffer(4L));

    DataFile dataFile = DataFiles
        .builder(PARTITION_SPEC)
        .withPath("/path/to/data-1.parquet")
        .withFileSizeInBytes(1234)
        .withPartitionPath("date=2018-06-08")
        .withMetrics(new Metrics(5L, null, valueCounts, nullValueCounts, lowerBounds, upperBounds))
        .withSplitOffsets(ImmutableList.of(4L))
        .build();

    try (Output out = new Output(new FileOutputStream(data))) {
      kryo.writeClassAndObject(out, dataFile);
      kryo.writeClassAndObject(out, dataFile.copy());
    }

    try (Input in = new Input(new FileInputStream(data))) {
      for (int i = 0; i < 2; i += 1) {
        Object obj = kryo.readClassAndObject(in);
        Assert.assertTrue("Should be a DataFile", obj instanceof DataFile);
        DataFile result = (DataFile) obj;
        Assert.assertEquals("Should match the serialized record path",
            dataFile.path(), result.path());
        Assert.assertEquals("Should match the serialized record format",
            dataFile.format(), result.format());
        Assert.assertEquals("Should match the serialized record partition",
            dataFile.partition().get(0, Object.class), result.partition().get(0, Object.class));
        Assert.assertEquals("Should match the serialized record count",
            dataFile.recordCount(), result.recordCount());
        Assert.assertEquals("Should match the serialized record size",
            dataFile.fileSizeInBytes(), result.fileSizeInBytes());
        Assert.assertEquals("Should match the serialized record value counts",
            dataFile.valueCounts(), result.valueCounts());
        Assert.assertEquals("Should match the serialized record null value counts",
            dataFile.nullValueCounts(), result.nullValueCounts());
        Assert.assertEquals("Should match the serialized record lower bounds",
            dataFile.lowerBounds(), result.lowerBounds());
        Assert.assertEquals("Should match the serialized record upper bounds",
            dataFile.upperBounds(), result.upperBounds());
        Assert.assertEquals("Should match the serialized record key metadata",
            dataFile.keyMetadata(), result.keyMetadata());
        Assert.assertEquals("Should match the serialized record offsets",
            dataFile.splitOffsets(), result.splitOffsets());
      }
    }
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
