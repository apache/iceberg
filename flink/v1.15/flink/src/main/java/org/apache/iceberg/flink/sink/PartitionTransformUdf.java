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
package org.apache.iceberg.flink.sink;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;

public class PartitionTransformUdf {

  public static class Truncate extends ScalarFunction {

    public static final String FUNCTION_NAME = "truncates";

    private static final Cache<Tuple2<Integer, Type>, Transform<Object, Object>> TRUNCATE_CACHE =
        Caffeine.newBuilder().build();

    private static Transform<Object, Object> truncateTransform(int num, Type type) {
      return TRUNCATE_CACHE.get(Tuple2.of(num, type), key -> Transforms.truncate(type, num));
    }

    public int eval(int num, int value) {
      Transform<Object, Object> truncate = truncateTransform(num, Types.IntegerType.get());
      return (int) truncate.apply(value);
    }

    public long eval(int num, long value) {
      Transform<Object, Object> truncate = truncateTransform(num, Types.LongType.get());
      return (long) truncate.apply(value);
    }

    public String eval(int num, String value) {
      Transform<Object, Object> truncate = truncateTransform(num, Types.StringType.get());
      return (String) truncate.apply(value);
    }

    public byte[] eval(int num, byte[] value) {
      Transform<Object, Object> truncate = truncateTransform(num, Types.BinaryType.get());
      ByteBuffer wrap = ByteBuffer.wrap(value);
      ByteBuffer byteBuffer = (ByteBuffer) truncate.apply(wrap);
      return ByteBuffers.toByteArray(byteBuffer);
    }

    public BigDecimal eval(int num, BigDecimal value) {
      Transform<Object, Object> truncate =
          truncateTransform(num, Types.DecimalType.of(value.precision(), value.scale()));
      return (BigDecimal) truncate.apply(value);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .outputTypeStrategy(
              callContext -> {
                LogicalTypeRoot typeRoot =
                    callContext.getArgumentDataTypes().get(1).getLogicalType().getTypeRoot();
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

    public static final String FUNCTION_NAME = "buckets";

    private static final Cache<Pair<Integer, Type>, Transform<Object, Integer>> BUCKET_CACHE =
        Caffeine.newBuilder().build();

    private static Transform<Object, Integer> bucketTransform(int num, Type type) {
      return BUCKET_CACHE.get(Pair.of(num, type), key -> Transforms.bucket(type, num));
    }

    public int eval(int num, int value) {
      Transform<Object, Integer> bucket = bucketTransform(num, Types.IntegerType.get());
      return bucket.apply(value);
    }

    public int eval(int num, long value) {
      Transform<Object, Integer> bucket = bucketTransform(num, Types.LongType.get());
      return bucket.apply(value);
    }

    public int eval(int num, BigDecimal value) {
      Transform<Object, Integer> bucket =
          bucketTransform(num, Types.DecimalType.of(value.precision(), value.scale()));
      return bucket.apply(value);
    }

    public int eval(int num, String value) {
      Transform<Object, Integer> bucket = bucketTransform(num, Types.StringType.get());
      return bucket.apply(value);
    }

    public int eval(int num, byte[] value) {
      ByteBuffer wrap = ByteBuffer.wrap(value);
      Transform<Object, Integer> bucket = bucketTransform(num, Types.BinaryType.get());
      return bucket.apply(wrap);
    }

    public int eval(int num, @DataTypeHint("DATE") LocalDate value) {
      Transform<Object, Integer> bucket = bucketTransform(num, Types.DateType.get());
      int days = DateTimeUtil.daysFromDate(value);
      return bucket.apply(days);
    }

    public int eval(int num, @DataTypeHint("TIME") LocalTime value) {
      Transform<Object, Integer> bucket = bucketTransform(num, Types.TimeType.get());
      long micros = DateTimeUtil.microsFromTime(value);
      return bucket.apply(micros);
    }

    public int eval(int num, @DataTypeHint(value = "TIMESTAMP") LocalDateTime value) {
      Transform<Object, Integer> bucket = bucketTransform(num, Types.TimestampType.withoutZone());
      long micros = DateTimeUtil.microsFromTimestamp(value);
      return bucket.apply(micros);
    }

    public int eval(int num, @DataTypeHint(value = "TIMESTAMP_LTZ") Instant value) {
      Transform<Object, Integer> bucket = bucketTransform(num, Types.TimestampType.withZone());
      long micros = DateTimeUtil.microsFromInstant(value);
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
