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

package org.apache.iceberg.mr.mapred;

import java.util.List;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

public class TestIcebergSchemaToTypeInfo {

  @Test
  public void testGeneratePrimitiveTypeInfo() throws Exception {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        required(8, "feature1", Types.BooleanType.get()),
        required(12, "lat", Types.FloatType.get()),
        required(15, "x", Types.LongType.get()),
        required(16, "date", Types.DateType.get()),
        required(17, "double", Types.DoubleType.get()),
        required(18, "binary", Types.BinaryType.get()));
    List<TypeInfo> types = IcebergSchemaToTypeInfo.getColumnTypes(schema);

    assertEquals("Converted TypeInfo should have the same number of columns.", 8, types.size());
    assertEquals("IntegerType converted incorrectly.",
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME), types.get(0));
    assertEquals("StringType converted incorrectly.",
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME), types.get(1));
    assertEquals("BooleanType converted incorrectly.",
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME), types.get(2));
    assertEquals("FloatType converted incorrectly.",
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME), types.get(3));
    assertEquals("LongType converted incorrectly.",
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME), types.get(4));
    assertEquals("DateType converted incorrectly.",
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME), types.get(5));
    assertEquals("DoubleType converted incorrectly.",
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME), types.get(6));
    assertEquals("BinaryType converted incorrectly.",
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BINARY_TYPE_NAME), types.get(7));
  }

  @Test
  public void testGenerateMapWithStringKeyTypeInfo() throws Exception {
    TypeInfo expected = TypeInfoFactory.getMapTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME));

    Schema schema = new Schema(
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties"));

    List<TypeInfo> types = IcebergSchemaToTypeInfo.getColumnTypes(schema);

    assertEquals("Converted TypeInfo should have the same number of columns.", 1, types.size());
    assertEquals("MapType converted incorrectly.", expected, types.get(0));
  }

  @Test
  public void testGenerateMapWithIntKeyTypeInfo() throws Exception {
    TypeInfo expected = TypeInfoFactory.getMapTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME));

    Schema schema = new Schema(
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.IntegerType.get(),
            Types.StringType.get()
        ), "string map of properties"));

    List<TypeInfo> types = IcebergSchemaToTypeInfo.getColumnTypes(schema);

    assertEquals("Converted TypeInfo should have the same number of columns.", 1, types.size());
    assertEquals("MapType converted incorrectly.", expected, types.get(0));
  }

  @Test
  public void testGenerateListTypeInfo() throws Exception {
    TypeInfo expected = TypeInfoFactory
        .getListTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME));
    Schema schema = new Schema(
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )));
    List<TypeInfo> types = IcebergSchemaToTypeInfo.getColumnTypes(schema);

    assertEquals("Converted TypeInfo should have the same number of columns.", 1, types.size());
    assertEquals("ListType converted incorrectly.", expected, types.get(0));
  }
}
