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

package org.apache.iceberg.parquet;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestParquetSchemaUtil {
  private static final Types.StructType SUPPORTED_PRIMITIVES = Types.StructType.of(
      required(100, "id", Types.LongType.get()),
      optional(101, "data", Types.StringType.get()),
      required(102, "b", Types.BooleanType.get()),
      optional(103, "i", Types.IntegerType.get()),
      required(104, "l", Types.LongType.get()),
      optional(105, "f", Types.FloatType.get()),
      required(106, "d", Types.DoubleType.get()),
      optional(107, "date", Types.DateType.get()),
      required(108, "ts", Types.TimestampType.withZone()),
      required(110, "s", Types.StringType.get()),
      required(112, "fixed", Types.FixedType.ofLength(7)),
      optional(113, "bytes", Types.BinaryType.get()),
      required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
      required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // spark's maximum precision
  );

  @Test
  public void testAssignIdsByNameMapping() {
    Types.StructType structType = Types.StructType.of(
        required(0, "id", Types.LongType.get()),
        optional(1, "list_of_maps",
            Types.ListType.ofOptional(2, Types.MapType.ofOptional(3, 4,
                Types.StringType.get(),
                SUPPORTED_PRIMITIVES))),
        optional(5, "map_of_lists",
            Types.MapType.ofOptional(6, 7,
                Types.StringType.get(),
                Types.ListType.ofOptional(8, SUPPORTED_PRIMITIVES))),
        required(9, "list_of_lists",
            Types.ListType.ofOptional(10, Types.ListType.ofOptional(11, SUPPORTED_PRIMITIVES))),
        required(12, "map_of_maps",
            Types.MapType.ofOptional(13, 14,
                Types.StringType.get(),
                Types.MapType.ofOptional(15, 16,
                    Types.StringType.get(),
                    SUPPORTED_PRIMITIVES))),
        required(17, "list_of_struct_of_nested_types", Types.ListType.ofOptional(19, Types.StructType.of(
            Types.NestedField.required(20, "m1", Types.MapType.ofOptional(21, 22,
                Types.StringType.get(),
                SUPPORTED_PRIMITIVES)),
            Types.NestedField.optional(23, "l1", Types.ListType.ofRequired(24, SUPPORTED_PRIMITIVES)),
            Types.NestedField.required(25, "l2", Types.ListType.ofRequired(26, SUPPORTED_PRIMITIVES)),
            Types.NestedField.optional(27, "m2", Types.MapType.ofOptional(28, 29,
                Types.StringType.get(),
                SUPPORTED_PRIMITIVES))
        )))
    );

    Schema schema = new Schema(TypeUtil.assignFreshIds(structType, new AtomicInteger(0)::incrementAndGet)
        .asStructType().fields());
    NameMapping nameMapping = MappingUtil.create(schema);
    MessageType messageTypeWithIds = ParquetSchemaUtil.convert(schema, "parquet_type");
    MessageType messageTypeWithIdsFromNameMapping = ParquetSchemaUtil
        .applyNameMapping(RemoveIds.removeIds(schema, "parquet_type"), nameMapping);

    Assert.assertEquals(messageTypeWithIds, messageTypeWithIdsFromNameMapping);
  }
}
