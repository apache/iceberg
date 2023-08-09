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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class AvroDataTest {

  protected abstract void writeAndValidate(Schema schema) throws IOException;

  protected static final StructType SUPPORTED_PRIMITIVES =
      StructType.of(
          required(100, "id", LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts", Types.TimestampType.withZone()),
          required(110, "s", Types.StringType.get()),
          required(111, "uuid", Types.UUIDType.get()),
          required(112, "fixed", Types.FixedType.ofLength(7)),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
          required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // spark's maximum precision
          );

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testSimpleStruct() throws IOException {
    writeAndValidate(TypeUtil.assignIncreasingFreshIds(new Schema(SUPPORTED_PRIMITIVES.fields())));
  }

  @Test
  public void testStructWithRequiredFields() throws IOException {
    writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(
            new Schema(
                Lists.transform(SUPPORTED_PRIMITIVES.fields(), Types.NestedField::asRequired))));
  }

  @Test
  public void testStructWithOptionalFields() throws IOException {
    writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(
            new Schema(
                Lists.transform(SUPPORTED_PRIMITIVES.fields(), Types.NestedField::asOptional))));
  }

  @Test
  public void testNestedStruct() throws IOException {
    writeAndValidate(
        TypeUtil.assignIncreasingFreshIds(new Schema(required(1, "struct", SUPPORTED_PRIMITIVES))));
  }

  @Test
  public void testArray() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(1, "data", ListType.ofOptional(2, Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testArrayOfStructs() throws IOException {
    Schema schema =
        TypeUtil.assignIncreasingFreshIds(
            new Schema(
                required(0, "id", LongType.get()),
                optional(1, "data", ListType.ofOptional(2, SUPPORTED_PRIMITIVES))));

    writeAndValidate(schema);
  }

  @Test
  public void testMap() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(
                1,
                "data",
                MapType.ofOptional(2, 3, Types.StringType.get(), Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testNumericMapKey() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(
                1, "data", MapType.ofOptional(2, 3, Types.LongType.get(), Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testComplexMapKey() throws IOException {
    Schema schema =
        new Schema(
            required(0, "id", LongType.get()),
            optional(
                1,
                "data",
                MapType.ofOptional(
                    2,
                    3,
                    Types.StructType.of(
                        required(4, "i", Types.IntegerType.get()),
                        optional(5, "s", Types.StringType.get())),
                    Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testMapOfStructs() throws IOException {
    Schema schema =
        TypeUtil.assignIncreasingFreshIds(
            new Schema(
                required(0, "id", LongType.get()),
                optional(
                    1,
                    "data",
                    MapType.ofOptional(2, 3, Types.StringType.get(), SUPPORTED_PRIMITIVES))));

    writeAndValidate(schema);
  }

  @Test
  public void testMixedTypes() throws IOException {
    StructType structType =
        StructType.of(
            required(0, "id", LongType.get()),
            optional(
                1,
                "list_of_maps",
                ListType.ofOptional(
                    2, MapType.ofOptional(3, 4, Types.StringType.get(), SUPPORTED_PRIMITIVES))),
            optional(
                5,
                "map_of_lists",
                MapType.ofOptional(
                    6, 7, Types.StringType.get(), ListType.ofOptional(8, SUPPORTED_PRIMITIVES))),
            required(
                9,
                "list_of_lists",
                ListType.ofOptional(10, ListType.ofOptional(11, SUPPORTED_PRIMITIVES))),
            required(
                12,
                "map_of_maps",
                MapType.ofOptional(
                    13,
                    14,
                    Types.StringType.get(),
                    MapType.ofOptional(15, 16, Types.StringType.get(), SUPPORTED_PRIMITIVES))),
            required(
                17,
                "list_of_struct_of_nested_types",
                ListType.ofOptional(
                    19,
                    StructType.of(
                        Types.NestedField.required(
                            20,
                            "m1",
                            MapType.ofOptional(
                                21, 22, Types.StringType.get(), SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            23, "l1", ListType.ofRequired(24, SUPPORTED_PRIMITIVES)),
                        Types.NestedField.required(
                            25, "l2", ListType.ofRequired(26, SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            27,
                            "m2",
                            MapType.ofOptional(
                                28, 29, Types.StringType.get(), SUPPORTED_PRIMITIVES))))));

    Schema schema =
        new Schema(
            TypeUtil.assignFreshIds(structType, new AtomicInteger(0)::incrementAndGet)
                .asStructType()
                .fields());

    writeAndValidate(schema);
  }

  @Test
  public void testTimestampWithoutZone() throws IOException {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE, "true"),
        () -> {
          Schema schema =
              TypeUtil.assignIncreasingFreshIds(
                  new Schema(
                      required(0, "id", LongType.get()),
                      optional(1, "ts_without_zone", Types.TimestampType.withoutZone())));

          writeAndValidate(schema);
        });
  }

  protected void withSQLConf(Map<String, String> conf, Action action) throws IOException {
    SQLConf sqlConf = SQLConf.get();

    Map<String, String> currentConfValues = Maps.newHashMap();
    conf.keySet()
        .forEach(
            confKey -> {
              if (sqlConf.contains(confKey)) {
                String currentConfValue = sqlConf.getConfString(confKey);
                currentConfValues.put(confKey, currentConfValue);
              }
            });

    conf.forEach(
        (confKey, confValue) -> {
          if (SQLConf.isStaticConfigKey(confKey)) {
            throw new RuntimeException("Cannot modify the value of a static config: " + confKey);
          }
          sqlConf.setConfString(confKey, confValue);
        });

    try {
      action.invoke();
    } finally {
      conf.forEach(
          (confKey, confValue) -> {
            if (currentConfValues.containsKey(confKey)) {
              sqlConf.setConfString(confKey, currentConfValues.get(confKey));
            } else {
              sqlConf.unsetConf(confKey);
            }
          });
    }
  }

  @FunctionalInterface
  protected interface Action {
    void invoke() throws IOException;
  }
}
