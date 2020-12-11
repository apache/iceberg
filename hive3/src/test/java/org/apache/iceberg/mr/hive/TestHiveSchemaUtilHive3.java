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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hive.TestHiveSchemaUtil;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveSchemaUtilHive3 extends TestHiveSchemaUtil {

  @Override
  protected List<FieldSchema> getSupportedFieldSchemas() {
    List<FieldSchema> fields = new ArrayList<>();
    fields.add(new FieldSchema("c_float", serdeConstants.FLOAT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_double", serdeConstants.DOUBLE_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_boolean", serdeConstants.BOOLEAN_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_int", serdeConstants.INT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_long", serdeConstants.BIGINT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_binary", serdeConstants.BINARY_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_string", serdeConstants.STRING_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_timestamp", serdeConstants.TIMESTAMP_TYPE_NAME, ""));
    // timestamp local tz only present in Hive3
    fields.add(new FieldSchema("c_timestamptz", serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_date", serdeConstants.DATE_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_decimal", serdeConstants.DECIMAL_TYPE_NAME + "(38,10)", ""));
    return fields;
  }

  @Override
  protected Schema getSchemaWithSupportedTypes() {
    return new Schema(
        optional(0, "c_float", Types.FloatType.get()),
        optional(1, "c_double", Types.DoubleType.get()),
        optional(2, "c_boolean", Types.BooleanType.get()),
        optional(3, "c_int", Types.IntegerType.get()),
        optional(4, "c_long", Types.LongType.get()),
        optional(5, "c_binary", Types.BinaryType.get()),
        optional(6, "c_string", Types.StringType.get()),
        optional(7, "c_timestamp", Types.TimestampType.withoutZone()),
        // timestamp local tz only present in Hive3
        optional(8, "c_timestamptz", Types.TimestampType.withZone()),
        optional(9, "c_date", Types.DateType.get()),
        optional(10, "c_decimal", Types.DecimalType.of(38, 10)));
  }
}
