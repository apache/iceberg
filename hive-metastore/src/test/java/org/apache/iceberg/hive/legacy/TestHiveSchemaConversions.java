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

package org.apache.iceberg.hive.legacy;

import java.util.List;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;


public class TestHiveSchemaConversions {
  @Test
  public void testPrimitiveTypes() {
    List<Type> primitives = Lists.newArrayList(
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimestampType.withoutZone(),
        Types.StringType.get(),
        Types.BinaryType.get(),
        Types.DecimalType.of(9, 4)
    );

    List<PrimitiveTypeInfo> hivePrimitives = Lists.newArrayList(
        TypeInfoFactory.booleanTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.floatTypeInfo,
        TypeInfoFactory.doubleTypeInfo,
        TypeInfoFactory.dateTypeInfo,
        TypeInfoFactory.timestampTypeInfo,
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.binaryTypeInfo,
        TypeInfoFactory.getDecimalTypeInfo(9, 4)
    );

    for (int i = 0; i < primitives.size(); i += 1) {
      Type icebergType = primitives.get(i);
      PrimitiveTypeInfo hiveType = hivePrimitives.get(i);
      Assert.assertEquals("Hive schema to primitive: " + hiveType, icebergType, HiveTypeUtil.convert(hiveType));
    }
  }

  @Test
  public void testConversions() {
    check("struct<1: a: optional int, 2: b: optional string>", "struct<a:int,b:string>");
    check("struct<5: a: optional map<string, map<string, double>>>", "struct<a:map<string,map<string,double>>>");
    check("struct<2: l1: optional list<boolean>>", "struct<l1:array<boolean>>");
    check("struct<3: l1: optional list<struct<1: l2: optional string>>>", "struct<l1:array<struct<l2:string>>>");
    check("list<map<string, list<map<string, list<double>>>>>", "array<map<string,array<map<string,array<double>>>>>");
    check("struct<" +
              "6: length: optional int, 7: count: optional int, " +
              "8: list: optional list<struct<1: lastword: optional string, 2: lastwordlength: optional int>>, " +
              "9: wordcounts: optional map<string, int>>",
          "struct<" +
              "length:int,count:int,list:array<struct<lastword:string,lastwordlength:int>>," +
              "wordcounts:map<string,int>>");
  }

  private static void check(String icebergTypeStr, String hiveTypeStr) {
    Type icebergType = HiveTypeUtil.convert(TypeInfoUtils.getTypeInfoFromTypeString(hiveTypeStr));
    Assert.assertEquals(icebergTypeStr, icebergType.toString());
  }
}
