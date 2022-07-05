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
 * KIND, either express or implied.  See the Licenet ideajoinet ideajoin for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

public class PartitionTransformUdf {

  public static class Truncate extends ScalarFunction {

    public int eval(int num, int value) {
      Transform<Integer, Integer> truncate = Transforms.truncate(Types.IntegerType.get(), num);
      return truncate.apply(value);
    }

    public long eval(int num, long value) {
      Transform<Long, Long> truncate = Transforms.truncate(Types.LongType.get(), num);
      return truncate.apply(value);
    }

    public String eval(int num, String value) {
      Transform<String, String> truncate = Transforms.truncate(Types.StringType.get(), num);
      return truncate.apply(value);
    }

    public byte[] eval(int num, byte[] value) {
      ByteBuffer wrap = ByteBuffer.wrap(value);
      Transform<ByteBuffer, ByteBuffer> truncate = Transforms.truncate(Types.BinaryType.get(), num);
      ByteBuffer byteBuffer = truncate.apply(wrap);
      byte[] out = new byte[byteBuffer.remaining()];
      byteBuffer.get(out, 0, out.length);
      return out;
    }

    public BigDecimal eval(int num, BigDecimal value) {
      Transform<BigDecimal, BigDecimal> truncate = Transforms.truncate(
          Types.DecimalType.of(value.precision(), value.scale()),
          num);
      return truncate.apply(value);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .outputTypeStrategy(callContext -> {
            LogicalTypeRoot typeRoot = callContext.getArgumentDataTypes().get(1).getLogicalType().getTypeRoot();
            if (typeRoot == LogicalTypeRoot.CHAR) {
              return Optional.of(DataTypes.STRING());
            } else if (typeRoot == LogicalTypeRoot.BINARY) {
              return Optional.of(DataTypes.BYTES());
            }
            return Optional.of(callContext.getArgumentDataTypes().get(1));
          })
          .build();
    }
  }

  public static class Bucket extends ScalarFunction {

    public int eval(int num, int value) {
      Transform<Object, Integer> bucket = Transforms.bucket(Types.IntegerType.get(), num);
      return bucket.apply(value);
    }

    public int eval(int num, long value) {
      Transform<Object, Integer> bucket = Transforms.bucket(Types.LongType.get(), num);
      return bucket.apply(value);
    }

    public int eval(int num, BigDecimal value) {
      Transform<Object, Integer> bucket =
          Transforms.bucket(Types.DecimalType.of(value.precision(), value.scale()), num);
      return bucket.apply(value);
    }

    public int eval(int num, String value) {
      Transform<Object, Integer> bucket = Transforms.bucket(Types.StringType.get(), num);
      return bucket.apply(value);
    }

    public int eval(int num, byte[] value) {
      ByteBuffer wrap = ByteBuffer.wrap(value);
      Transform<Object, Integer> bucket = Transforms.bucket(Types.BinaryType.get(), num);
      return bucket.apply(wrap);
    }

    public int eval(int num, @DataTypeHint("DATE") LocalDate value) {
      Transform<Object, Integer> bucket = Transforms.bucket(Types.DateType.get(), num);
      int days = (int) value.toEpochDay();
      return bucket.apply(days);
    }

    public int eval(int num, @DataTypeHint("TIME") LocalTime value) {
      Types.TimeType type = Types.TimeType.get();
      Transform<Object, Integer> bucket = Transforms.bucket(type, num);
      long micros = TimeUnit.NANOSECONDS.toMicros(value.toNanoOfDay());
      return bucket.apply(micros);
    }

    public int eval(int num, @DataTypeHint(value = "TIMESTAMP") LocalDateTime value) {
      Types.TimestampType type = Types.TimestampType.withoutZone();
      Instant instant = value.atZone(ZoneOffset.UTC).toInstant();
      long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) +
          TimeUnit.NANOSECONDS.toMicros(instant.getNano());
      Transform<Object, Integer> bucket = Transforms.bucket(type, num);
      return bucket.apply(micros);
    }

    public int eval(int num, @DataTypeHint(value = "TIMESTAMP_LTZ") Instant value) {
      Types.TimestampType type = Types.TimestampType.withZone();
      long micros = TimeUnit.SECONDS.toMicros(value.getEpochSecond()) +
          TimeUnit.NANOSECONDS.toMicros(value.getNano());
      Transform<Object, Integer> bucket = Transforms.bucket(type, num);
      return bucket.apply(micros);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .outputTypeStrategy(callContext -> Optional.of(DataTypes.INT().notNull()))
          .build();
    }
  }

}
