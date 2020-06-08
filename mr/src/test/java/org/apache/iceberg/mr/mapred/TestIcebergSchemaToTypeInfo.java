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

import java.util.ArrayList;
import java.util.Arrays;
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

    assertEquals(8, types.size());
  }

  @Test
  public void testGenerateMapTypeInfo() throws Exception {
    TypeInfo expected = TypeInfoFactory.getMapTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME));

    Schema schema = new Schema(
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties"));

    List<TypeInfo> types = IcebergSchemaToTypeInfo.getColumnTypes(schema);

    assertEquals(1, types.size());
    assertEquals(expected, types.get(0));
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

    assertEquals(1, types.size());
    assertEquals(expected, types.get(0));
  }

  @Test
  public void testGenerateMapAndStructTypeInfo() throws Exception {
    List<String> names1 = new ArrayList<>(Arrays.asList("address", "city", "state", "zip"));
    List<TypeInfo> typeInfo1 = new ArrayList<>(Arrays.asList(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME)
    ));
    TypeInfo mapKeyStructExpected = TypeInfoFactory.getStructTypeInfo(names1, typeInfo1);

    List<String> names2 = new ArrayList<>(Arrays.asList("lat", "long"));
    List<TypeInfo> typeInfo2 = new ArrayList<>(Arrays.asList(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME)
    ));
    TypeInfo mapValueStructExpected = TypeInfoFactory.getStructTypeInfo(names2, typeInfo2);

    TypeInfo expected = TypeInfoFactory.getMapTypeInfo(mapKeyStructExpected, mapValueStructExpected);

    Schema schema = new Schema(
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "lat", Types.FloatType.get()),
                required(13, "long", Types.FloatType.get())
            )), "map of address to coordinate"));
    List<TypeInfo> types = IcebergSchemaToTypeInfo.getColumnTypes(schema);

    assertEquals(1, types.size());
    assertEquals(expected, types.get(0));
  }

  @Test
  public void testComplexSchema() throws Exception {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "preferences", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "feature2", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties")
    );
    List<TypeInfo> types = IcebergSchemaToTypeInfo.getColumnTypes(schema);

    assertEquals(5, types.size());
    assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME), types.get(0));
    assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME), types.get(1));

    List<String> preferencesNames = new ArrayList<>(Arrays.asList("feature1", "feature2"));
    List<TypeInfo> preferencesTypeInfo = new ArrayList<>(Arrays.asList(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME)
    ));
    TypeInfo preferencesTypeExpected = TypeInfoFactory.getStructTypeInfo(preferencesNames, preferencesTypeInfo);
    assertEquals(preferencesTypeExpected, types.get(2));
    assertEquals(TypeInfoFactory.getListTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME)), types.get(3));

    TypeInfo propertiesExpected = TypeInfoFactory.getMapTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME));
    assertEquals(propertiesExpected, types.get(4));
  }
}
