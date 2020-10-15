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

package org.apache.iceberg.mr.hive;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.iceberg.data.Record;

public class TestHiveIcebergSerDeHive3 extends TestHiveIcebergSerDe {

  protected List<ObjectInspector> objectInspectors() {
    return Arrays.asList(
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector
    );
  }

  protected List<Object> values(Record record) {
    OffsetDateTime offsetDateTime = record.get(6, OffsetDateTime.class);
    Timestamp timestampForOffsetDateTime = Timestamp.ofEpochMilli(offsetDateTime.toInstant().toEpochMilli());

    LocalDateTime localDateTime = record.get(7, LocalDateTime.class);
    Timestamp timestampForLocalDateTime =
        Timestamp.ofEpochMilli(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());

    ByteBuffer byteBuffer = record.get(11, ByteBuffer.class);
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.mark();
    byteBuffer.get(bytes);
    byteBuffer.reset();

    return Arrays.asList(
        new BooleanWritable(Boolean.TRUE),
        new IntWritable(record.get(1, Integer.class)),
        new LongWritable(record.get(2, Long.class)),
        new FloatWritable(record.get(3, Float.class)),
        new DoubleWritable(record.get(4, Double.class)),
        new DateWritableV2((int) record.get(5, LocalDate.class).toEpochDay()),
        // TimeType is not supported
        // new Timestamp()
        new TimestampWritableV2(timestampForOffsetDateTime),
        new TimestampWritableV2(timestampForLocalDateTime),
        new Text(record.get(8, String.class)),
        new Text(record.get(9, UUID.class).toString()),
        new BytesWritable(record.get(10, byte[].class)),
        new BytesWritable(bytes),
        new HiveDecimalWritable(HiveDecimal.create(record.get(12, BigDecimal.class)))
    );
  }
}
