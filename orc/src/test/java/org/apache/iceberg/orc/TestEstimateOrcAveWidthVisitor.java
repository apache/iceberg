/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.orc;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestEstimateOrcAveWidthVisitor {

  // all supported fields, except for UUID which is on deprecation path: see https://github.com/apache/iceberg/pull/1611
  // as well as Types.TimeType and Types.TimestampType.withoutZone as both are not supported by Spark
  protected static final Types.NestedField ID_FIELD = required(1, "id", Types.IntegerType.get());
  protected static final Types.NestedField DATA_FIELD = optional(2, "data", Types.StringType.get());
  protected static final Types.NestedField FLOAT_FIELD = required(3, "float", Types.FloatType.get());
  protected static final Types.NestedField DOUBLE_FIELD = optional(4, "double", Types.DoubleType.get());
  protected static final Types.NestedField DECIMAL_FIELD = optional(5, "decimal", Types.DecimalType.of(5, 3));
  protected static final Types.NestedField FIXED_FIELD = optional(7, "fixed", Types.FixedType.ofLength(4));
  protected static final Types.NestedField BINARY_FIELD = optional(8, "binary", Types.BinaryType.get());
  protected static final Types.NestedField FLOAT_LIST = optional(9, "floatlist",
      Types.ListType.ofRequired(10, Types.FloatType.get()));
  protected static final Types.NestedField LONG_FIELD = optional(11, "long", Types.LongType.get());

  protected static final Types.NestedField MAP_FIELD_1 = optional(17, "map1",
      Types.MapType.ofOptional(18, 19, Types.FloatType.get(), Types.StringType.get())
  );
  protected static final Types.NestedField MAP_FIELD_2 = optional(20, "map2",
      Types.MapType.ofOptional(21, 22, Types.IntegerType.get(), Types.DoubleType.get())
  );
  protected static final Types.NestedField STRUCT_FIELD = optional(23, "structField", Types.StructType.of(
      required(24, "booleanField", Types.BooleanType.get()),
      optional(25, "date", Types.DateType.get()),
      optional(27, "timestamp", Types.TimestampType.withZone())
  ));

  private static final Map<Types.NestedField, Integer> FIELDS_WITH_NAN_COUNT_TO_ID = ImmutableMap.of(
      FLOAT_FIELD, 3, DOUBLE_FIELD, 4, FLOAT_LIST, 10, MAP_FIELD_1, 18, MAP_FIELD_2, 22
  );

  // create a schema with all supported fields
  protected static final Schema SCHEMA = new Schema(
      ID_FIELD,
      DATA_FIELD,
      FLOAT_FIELD,
      DOUBLE_FIELD,
      DECIMAL_FIELD,
      FIXED_FIELD,
      BINARY_FIELD,
      FLOAT_LIST,
      LONG_FIELD,
      MAP_FIELD_1,
      MAP_FIELD_2,
      STRUCT_FIELD
  );

  @Test
  public void testEstimateDataAveWidth() {
    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(SCHEMA);

    long estimateLength = OrcSchemaVisitor.visitSchema(orcSchema, new EstimateOrcAveWidthVisitor())
        .stream().reduce(Integer::sum).orElse(1);

    Assert.assertEquals(602,estimateLength);
  }
}
