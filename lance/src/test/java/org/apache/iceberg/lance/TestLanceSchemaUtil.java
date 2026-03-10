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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link LanceSchemaUtil}. */
public class TestLanceSchemaUtil {

  @Test
  public void testPrimitiveTypesToArrow() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "active", Types.BooleanType.get()),
            Types.NestedField.optional(4, "score", Types.DoubleType.get()),
            Types.NestedField.optional(5, "rating", Types.FloatType.get()),
            Types.NestedField.required(6, "count", Types.LongType.get()));

    org.apache.arrow.vector.types.pojo.Schema arrowSchema = LanceSchemaUtil.toArrow(schema);

    List<Field> fields = arrowSchema.getFields();
    assertThat(fields).hasSize(6);

    assertThat(fields.get(0).getName()).isEqualTo("id");
    assertThat(fields.get(0).getType()).isInstanceOf(ArrowType.Int.class);
    assertThat(((ArrowType.Int) fields.get(0).getType()).getBitWidth()).isEqualTo(32);
    assertThat(fields.get(0).isNullable()).isFalse();

    assertThat(fields.get(1).getName()).isEqualTo("name");
    assertThat(fields.get(1).getType()).isInstanceOf(ArrowType.Utf8.class);
    assertThat(fields.get(1).isNullable()).isTrue();

    assertThat(fields.get(2).getName()).isEqualTo("active");
    assertThat(fields.get(2).getType()).isInstanceOf(ArrowType.Bool.class);

    assertThat(fields.get(3).getName()).isEqualTo("score");
    ArrowType.FloatingPoint fp64 = (ArrowType.FloatingPoint) fields.get(3).getType();
    assertThat(fp64.getPrecision()).isEqualTo(FloatingPointPrecision.DOUBLE);

    assertThat(fields.get(4).getName()).isEqualTo("rating");
    ArrowType.FloatingPoint fp32 = (ArrowType.FloatingPoint) fields.get(4).getType();
    assertThat(fp32.getPrecision()).isEqualTo(FloatingPointPrecision.SINGLE);

    assertThat(fields.get(5).getName()).isEqualTo("count");
    assertThat(((ArrowType.Int) fields.get(5).getType()).getBitWidth()).isEqualTo(64);
  }

  @Test
  public void testTemporalTypesToArrow() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "date_col", Types.DateType.get()),
            Types.NestedField.optional(2, "time_col", Types.TimeType.get()),
            Types.NestedField.optional(3, "ts_col", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "tsz_col", Types.TimestampType.withZone()));

    org.apache.arrow.vector.types.pojo.Schema arrowSchema = LanceSchemaUtil.toArrow(schema);
    List<Field> fields = arrowSchema.getFields();

    assertThat(fields.get(0).getType()).isInstanceOf(ArrowType.Date.class);
    assertThat(((ArrowType.Date) fields.get(0).getType()).getUnit()).isEqualTo(DateUnit.DAY);

    assertThat(fields.get(1).getType()).isInstanceOf(ArrowType.Time.class);
    assertThat(((ArrowType.Time) fields.get(1).getType()).getUnit()).isEqualTo(TimeUnit.MICROSECOND);

    assertThat(fields.get(2).getType()).isInstanceOf(ArrowType.Timestamp.class);
    ArrowType.Timestamp ts = (ArrowType.Timestamp) fields.get(2).getType();
    assertThat(ts.getUnit()).isEqualTo(TimeUnit.MICROSECOND);
    assertThat(ts.getTimezone()).isNull();

    assertThat(fields.get(3).getType()).isInstanceOf(ArrowType.Timestamp.class);
    ArrowType.Timestamp tsz = (ArrowType.Timestamp) fields.get(3).getType();
    assertThat(tsz.getTimezone()).isEqualTo("UTC");
  }

  @Test
  public void testDecimalAndFixedTypesToArrow() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "decimal_col", Types.DecimalType.of(18, 6)),
            Types.NestedField.optional(2, "fixed_col", Types.FixedType.ofLength(10)),
            Types.NestedField.optional(3, "uuid_col", Types.UUIDType.get()),
            Types.NestedField.optional(4, "binary_col", Types.BinaryType.get()));

    org.apache.arrow.vector.types.pojo.Schema arrowSchema = LanceSchemaUtil.toArrow(schema);
    List<Field> fields = arrowSchema.getFields();

    ArrowType.Decimal decType = (ArrowType.Decimal) fields.get(0).getType();
    assertThat(decType.getPrecision()).isEqualTo(18);
    assertThat(decType.getScale()).isEqualTo(6);

    ArrowType.FixedSizeBinary fixedType = (ArrowType.FixedSizeBinary) fields.get(1).getType();
    assertThat(fixedType.getByteWidth()).isEqualTo(10);

    // UUID maps to FixedSizeBinary(16)
    ArrowType.FixedSizeBinary uuidType = (ArrowType.FixedSizeBinary) fields.get(2).getType();
    assertThat(uuidType.getByteWidth()).isEqualTo(16);

    assertThat(fields.get(3).getType()).isInstanceOf(ArrowType.Binary.class);
  }

  @Test
  public void testNestedTypesToArrow() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "list_col",
                Types.ListType.ofOptional(2, Types.StringType.get())),
            Types.NestedField.optional(
                3,
                "struct_col",
                Types.StructType.of(
                    Types.NestedField.required(4, "x", Types.IntegerType.get()),
                    Types.NestedField.optional(5, "y", Types.StringType.get()))));

    org.apache.arrow.vector.types.pojo.Schema arrowSchema = LanceSchemaUtil.toArrow(schema);
    List<Field> fields = arrowSchema.getFields();

    assertThat(fields.get(0).getType()).isInstanceOf(ArrowType.List.class);
    assertThat(fields.get(0).getChildren()).hasSize(1);

    assertThat(fields.get(1).getType()).isInstanceOf(ArrowType.Struct.class);
    assertThat(fields.get(1).getChildren()).hasSize(2);
    assertThat(fields.get(1).getChildren().get(0).getName()).isEqualTo("x");
    assertThat(fields.get(1).getChildren().get(1).getName()).isEqualTo("y");
  }

  @Test
  public void testRoundTripPrimitiveSchema() {
    Schema original =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.DoubleType.get()));

    org.apache.arrow.vector.types.pojo.Schema arrowSchema = LanceSchemaUtil.toArrow(original);
    Schema roundTripped = LanceSchemaUtil.toIceberg(arrowSchema);

    // Verify field names and types match
    assertThat(roundTripped.columns()).hasSize(3);
    assertThat(roundTripped.columns().get(0).name()).isEqualTo("id");
    assertThat(roundTripped.columns().get(0).type().typeId()).isEqualTo(Types.IntegerType.get().typeId());
    assertThat(roundTripped.columns().get(1).name()).isEqualTo("name");
    assertThat(roundTripped.columns().get(1).type().typeId()).isEqualTo(Types.StringType.get().typeId());
    assertThat(roundTripped.columns().get(2).name()).isEqualTo("value");
    assertThat(roundTripped.columns().get(2).type().typeId()).isEqualTo(Types.DoubleType.get().typeId());
  }

  @Test
  public void testNullSchemaThrows() {
    assertThatThrownBy(() -> LanceSchemaUtil.toArrow(null))
        .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> LanceSchemaUtil.toIceberg(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testMapTypeToArrow() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1,
                "map_col",
                Types.MapType.ofOptional(
                    2, 3, Types.StringType.get(), Types.IntegerType.get())));

    org.apache.arrow.vector.types.pojo.Schema arrowSchema = LanceSchemaUtil.toArrow(schema);
    List<Field> fields = arrowSchema.getFields();

    assertThat(fields.get(0).getType()).isInstanceOf(ArrowType.Map.class);
    assertThat(fields.get(0).getChildren()).hasSize(1); // entries field
  }
}
