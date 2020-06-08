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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestFlinkSchemaUtil {

  @Test
  public void testConvertFlinkSchemaToIcebergSchema() {
    TableSchema flinkSchema = TableSchema.builder()
        .field("id", DataTypes.INT().notNull())
        .field("name", DataTypes.STRING()) /* optional by default */
        .field("salary", DataTypes.DOUBLE().notNull())
        .field("locations", DataTypes.MAP(DataTypes.STRING(),
            DataTypes.ROW(DataTypes.FIELD("posX", DataTypes.DOUBLE().notNull(), "X field"),
                DataTypes.FIELD("posY", DataTypes.DOUBLE().notNull(), "Y field"))))
        .field("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable())
        .field("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable())
        .field("char", DataTypes.CHAR(10).notNull())
        .field("varchar", DataTypes.VARCHAR(10).notNull())
        .field("boolean", DataTypes.BOOLEAN().nullable())
        .field("tinyint", DataTypes.TINYINT())
        .field("smallint", DataTypes.SMALLINT())
        .field("bigint", DataTypes.BIGINT())
        .field("varbinary", DataTypes.VARBINARY(10))
        .field("binary", DataTypes.BINARY(10))
        .field("time", DataTypes.TIME())
        .field("timestampWithoutZone", DataTypes.TIMESTAMP())
        .field("timestampWithZone", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
        .field("date", DataTypes.DATE())
        .field("decimal", DataTypes.DECIMAL(2, 2))
        .field("decimal2", DataTypes.DECIMAL(38, 2))
        .field("decimal3", DataTypes.DECIMAL(10, 1))
        .build();

    Schema actualSchema = FlinkSchemaUtil.convert(flinkSchema);
    Schema expectedSchema = new Schema(
        Types.NestedField.required(0, "id", Types.IntegerType.get(), null),
        Types.NestedField.optional(1, "name", Types.StringType.get(), null),
        Types.NestedField.required(2, "salary", Types.DoubleType.get(), null),
        Types.NestedField.optional(3, "locations", Types.MapType.ofOptional(23, 24,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(21, "posX", Types.DoubleType.get(), "X field"),
                Types.NestedField.required(22, "posY", Types.DoubleType.get(), "Y field")
            ))),
        Types.NestedField.optional(4, "strArray", Types.ListType.ofOptional(25, Types.StringType.get())),
        Types.NestedField.optional(5, "intArray", Types.ListType.ofOptional(26, Types.IntegerType.get())),
        Types.NestedField.required(6, "char", Types.StringType.get()),
        Types.NestedField.required(7, "varchar", Types.StringType.get()),
        Types.NestedField.optional(8, "boolean", Types.BooleanType.get()),
        Types.NestedField.optional(9, "tinyint", Types.IntegerType.get()),
        Types.NestedField.optional(10, "smallint", Types.IntegerType.get()),
        Types.NestedField.optional(11, "bigint", Types.LongType.get()),
        Types.NestedField.optional(12, "varbinary", Types.BinaryType.get()),
        Types.NestedField.optional(13, "binary", Types.FixedType.ofLength(10)),
        Types.NestedField.optional(14, "time", Types.TimeType.get()),
        Types.NestedField.optional(15, "timestampWithoutZone", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(16, "timestampWithZone", Types.TimestampType.withZone()),
        Types.NestedField.optional(17, "date", Types.DateType.get()),
        Types.NestedField.optional(18, "decimal", Types.DecimalType.of(2, 2)),
        Types.NestedField.optional(19, "decimal2", Types.DecimalType.of(38, 2)),
        Types.NestedField.optional(20, "decimal3", Types.DecimalType.of(10, 1))
    );

    Assert.assertEquals(expectedSchema, actualSchema);
  }
}
