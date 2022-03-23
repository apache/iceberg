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

package org.apache.iceberg.orc;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestEstimateOrcAvgWidthVisitor {

  // all supported fields
  protected static final Types.NestedField ID_FIELD = required(1, "id", Types.IntegerType.get());
  protected static final Types.NestedField DATA_FIELD = optional(2, "data", Types.StringType.get());
  protected static final Types.NestedField FLOAT_FIELD = required(3, "float", Types.FloatType.get());
  protected static final Types.NestedField DOUBLE_FIELD = optional(4, "double", Types.DoubleType.get());
  protected static final Types.NestedField DECIMAL_FIELD = optional(5, "decimal", Types.DecimalType.of(5, 3));
  protected static final Types.NestedField FIXED_FIELD = optional(7, "fixed", Types.FixedType.ofLength(4));
  protected static final Types.NestedField BINARY_FIELD = optional(8, "binary", Types.BinaryType.get());
  protected static final Types.NestedField FLOAT_LIST_FIELD = optional(9, "floatList",
      Types.ListType.ofRequired(10, Types.FloatType.get()));
  protected static final Types.NestedField LONG_FIELD = optional(11, "long", Types.LongType.get());
  protected static final Types.NestedField BOOLEAN_FIELD = optional(12, "boolean", Types.BooleanType.get());
  protected static final Types.NestedField TIMESTAMP_ZONE_FIELD = optional(13, "timestampZone",
      Types.TimestampType.withZone());
  protected static final Types.NestedField TIMESTAMP_FIELD = optional(14, "timestamp",
      Types.TimestampType.withoutZone());
  protected static final Types.NestedField DATE_FIELD = optional(15, "date", Types.DateType.get());
  protected static final Types.NestedField UUID_FIELD = required(16, "uuid", Types.UUIDType.get());

  protected static final Types.NestedField MAP_FIELD_1 = optional(17, "map1",
      Types.MapType.ofOptional(18, 19, Types.FloatType.get(), Types.StringType.get())
  );
  protected static final Types.NestedField MAP_FIELD_2 = optional(20, "map2",
      Types.MapType.ofOptional(21, 22, Types.IntegerType.get(), Types.DoubleType.get())
  );
  protected static final Types.NestedField STRUCT_FIELD = optional(23, "struct", Types.StructType.of(
      required(24, "booleanField", Types.BooleanType.get()),
      optional(25, "date", Types.DateType.get()),
      optional(27, "timestamp", Types.TimestampType.withZone())
  ));

  @Test
  public void testEstimateDataAveWidth() {
    // create a schema with integer field
    Schema schemaWithInteger = new Schema(
        ID_FIELD
    );
    TypeDescription orcSchemaWithInteger = ORCSchemaUtil.convert(schemaWithInteger);
    long estimateLength = getEstimateLength(orcSchemaWithInteger);
    Assert.assertEquals("Estimated average length of integer must be 8.", 8, estimateLength);

    // create a schema with string field
    Schema schemaWithString = new Schema(
        DATA_FIELD
    );
    TypeDescription orcSchemaWithString = ORCSchemaUtil.convert(schemaWithString);
    estimateLength = getEstimateLength(orcSchemaWithString);
    Assert.assertEquals("Estimated average length of string must be 128.", 128, estimateLength);

    // create a schema with float field
    Schema schemaWithFloat = new Schema(
        FLOAT_FIELD
    );
    TypeDescription orcSchemaWithFloat = ORCSchemaUtil.convert(schemaWithFloat);
    estimateLength = getEstimateLength(orcSchemaWithFloat);
    Assert.assertEquals("Estimated average length of float must be 8.", 8, estimateLength);

    // create a schema with double field
    Schema schemaWithDouble = new Schema(
        DOUBLE_FIELD
    );
    TypeDescription orcSchemaWithDouble = ORCSchemaUtil.convert(schemaWithDouble);
    estimateLength = getEstimateLength(orcSchemaWithDouble);
    Assert.assertEquals("Estimated average length of double must be 8.", 8, estimateLength);

    // create a schema with decimal field
    Schema schemaWithDecimal = new Schema(
        DECIMAL_FIELD
    );
    TypeDescription orcSchemaWithDecimal = ORCSchemaUtil.convert(schemaWithDecimal);
    estimateLength = getEstimateLength(orcSchemaWithDecimal);
    Assert.assertEquals("Estimated average length of decimal must be 7.", 7, estimateLength);

    // create a schema with fixed field
    Schema schemaWithFixed = new Schema(
        FIXED_FIELD
    );
    TypeDescription orcSchemaWithFixed = ORCSchemaUtil.convert(schemaWithFixed);
    estimateLength = getEstimateLength(orcSchemaWithFixed);
    Assert.assertEquals("Estimated average length of fixed must be 128.", 128, estimateLength);

    // create a schema with binary field
    Schema schemaWithBinary = new Schema(
        BINARY_FIELD
    );
    TypeDescription orcSchemaWithBinary = ORCSchemaUtil.convert(schemaWithBinary);
    estimateLength = getEstimateLength(orcSchemaWithBinary);
    Assert.assertEquals("Estimated average length of binary must be 128.", 128, estimateLength);

    // create a schema with list field
    Schema schemaWithList = new Schema(
        FLOAT_LIST_FIELD
    );
    TypeDescription orcSchemaWithList = ORCSchemaUtil.convert(schemaWithList);
    estimateLength = getEstimateLength(orcSchemaWithList);
    Assert.assertEquals("Estimated average length of list must be 8.", 8, estimateLength);

    // create a schema with long field
    Schema schemaWithLong = new Schema(
        LONG_FIELD
    );
    TypeDescription orcSchemaWithLong = ORCSchemaUtil.convert(schemaWithLong);
    estimateLength = getEstimateLength(orcSchemaWithLong);
    Assert.assertEquals("Estimated average length of long must be 8.", 8, estimateLength);

    // create a schema with boolean field
    Schema schemaWithBoolean = new Schema(
        BOOLEAN_FIELD
    );
    TypeDescription orcSchemaWithBoolean = ORCSchemaUtil.convert(schemaWithBoolean);
    estimateLength = getEstimateLength(orcSchemaWithBoolean);
    Assert.assertEquals("Estimated average length of boolean must be 8.", 8, estimateLength);

    // create a schema with timestamps with zone field
    Schema schemaWithTimestampWithZone = new Schema(
        TIMESTAMP_ZONE_FIELD
    );
    TypeDescription orcSchemaWithTimestampWithZone = ORCSchemaUtil.convert(schemaWithTimestampWithZone);
    estimateLength = getEstimateLength(orcSchemaWithTimestampWithZone);
    Assert.assertEquals("Estimated average length of timestamps with zone must be 12.", 12, estimateLength);

    // create a schema with timestamp field
    Schema schemaWithTimestamp = new Schema(
        TIMESTAMP_FIELD
    );
    TypeDescription orcSchemaWithTimestamp = ORCSchemaUtil.convert(schemaWithTimestamp);
    estimateLength = getEstimateLength(orcSchemaWithTimestamp);
    Assert.assertEquals("Estimated average length of timestamp must be 12.", 12, estimateLength);

    // create a schema with date field
    Schema schemaWithDate = new Schema(
        DATE_FIELD
    );
    TypeDescription orcSchemaWithDate = ORCSchemaUtil.convert(schemaWithDate);
    estimateLength = getEstimateLength(orcSchemaWithDate);
    Assert.assertEquals("Estimated average length of date must be 8.", 8, estimateLength);

    // create a schema with uuid field
    Schema schemaWithUUID = new Schema(
        UUID_FIELD
    );
    TypeDescription orcSchemaWithUUID = ORCSchemaUtil.convert(schemaWithUUID);
    estimateLength = getEstimateLength(orcSchemaWithUUID);
    Assert.assertEquals("Estimated average length of uuid must be 128.", 128, estimateLength);

    // create a schema with map field
    Schema schemaWithMap = new Schema(
        MAP_FIELD_1
    );
    TypeDescription orcSchemaWithMap = ORCSchemaUtil.convert(schemaWithMap);
    estimateLength = getEstimateLength(orcSchemaWithMap);
    Assert.assertEquals("Estimated average length of map must be 136.", 136, estimateLength);

    // create a schema with struct field
    Schema schemaWithStruct = new Schema(
        STRUCT_FIELD
    );
    TypeDescription orcSchemaWithStruct = ORCSchemaUtil.convert(schemaWithStruct);
    estimateLength = getEstimateLength(orcSchemaWithStruct);
    Assert.assertEquals("Estimated average length of struct must be 28.", 28, estimateLength);

    // create a schema with all supported fields
    Schema schemaWithFUll = new Schema(
        ID_FIELD,
        DATA_FIELD,
        FLOAT_FIELD,
        DOUBLE_FIELD,
        DECIMAL_FIELD,
        FIXED_FIELD,
        BINARY_FIELD,
        FLOAT_LIST_FIELD,
        LONG_FIELD,
        MAP_FIELD_1,
        MAP_FIELD_2,
        STRUCT_FIELD
    );
    TypeDescription orcSchemaWithFull = ORCSchemaUtil.convert(schemaWithFUll);
    estimateLength = getEstimateLength(orcSchemaWithFull);
    Assert.assertEquals("Estimated average length of the row must be 611.", 611, estimateLength);
  }

  private Integer getEstimateLength(TypeDescription orcSchemaWithDate) {
    return OrcSchemaVisitor.visitSchema(orcSchemaWithDate, new EstimateOrcAvgWidthVisitor())
        .stream().reduce(Integer::sum).orElse(0);
  }
}
