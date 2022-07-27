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
package org.apache.iceberg.pig;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class SchemaUtilTest {

  @Test
  public void testPrimitive() throws IOException {
    Schema icebergSchema =
        new Schema(
            optional(1, "b", BooleanType.get()),
            optional(2, "i", IntegerType.get()),
            optional(3, "l", LongType.get()),
            optional(4, "f", FloatType.get()),
            optional(5, "d", DoubleType.get()),
            optional(6, "dec", DecimalType.of(0, 2)),
            optional(7, "s", StringType.get()),
            optional(8, "bi", BinaryType.get()));

    ResourceSchema pigSchema = SchemaUtil.convert(icebergSchema);
    assertEquals(
        "b:boolean,i:int,l:long,f:float,d:double,dec:bigdecimal,s:chararray,bi:bytearray",
        pigSchema.toString());
  }

  @Test
  public void testComplex() throws IOException {
    convertToPigSchema(
        new Schema(
            optional(1, "bag", ListType.ofOptional(2, BooleanType.get())),
            optional(3, "map", MapType.ofOptional(4, 5, StringType.get(), DoubleType.get())),
            optional(
                6,
                "tuple",
                StructType.of(
                    optional(7, "i", IntegerType.get()), optional(8, "f", FloatType.get())))),
        "bag:{(boolean)},map:[double],tuple:(i:int,f:float)",
        null);
  }

  @Test
  public void invalidMap() {
    Assertions.assertThatThrownBy(
            () ->
                convertToPigSchema(
                    new Schema(
                        optional(
                            1,
                            "invalid",
                            MapType.ofOptional(2, 3, IntegerType.get(), DoubleType.get()))),
                    "",
                    ""))
        .isInstanceOf(FrontendException.class)
        .hasMessageContaining("Unsupported map key type: int");
  }

  @Test
  public void nestedMaps() throws IOException {
    convertToPigSchema(
        new Schema(
            optional(
                1,
                "nested",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    MapType.ofOptional(
                        4,
                        5,
                        StringType.get(),
                        MapType.ofOptional(6, 7, StringType.get(), DecimalType.of(10, 2)))))),
        "nested:[[[bigdecimal]]]",
        "");
  }

  @Test
  public void nestedBags() throws IOException {
    convertToPigSchema(
        new Schema(
            optional(
                1,
                "nested",
                ListType.ofOptional(
                    2, ListType.ofOptional(3, ListType.ofOptional(4, DoubleType.get()))))),
        "nested:{({({(double)})})}",
        "");
  }

  @Test
  public void nestedTuples() throws IOException {
    convertToPigSchema(
        new Schema(
            optional(
                1,
                "first",
                StructType.of(
                    optional(
                        2,
                        "second",
                        StructType.of(
                            optional(
                                3,
                                "third",
                                StructType.of(optional(4, "val", StringType.get())))))))),
        "first:(second:(third:(val:chararray)))",
        "");
  }

  @Test
  public void complexNested() throws IOException {
    convertToPigSchema(
        new Schema(
            optional(
                1,
                "t",
                StructType.of(
                    optional(
                        2,
                        "b",
                        ListType.ofOptional(
                            3,
                            StructType.of(
                                optional(4, "i", IntegerType.get()),
                                optional(5, "s", StringType.get())))))),
            optional(
                6,
                "m1",
                MapType.ofOptional(
                    7,
                    8,
                    StringType.get(),
                    StructType.of(
                        optional(9, "b", ListType.ofOptional(10, BinaryType.get())),
                        optional(
                            11,
                            "m2",
                            MapType.ofOptional(12, 13, StringType.get(), IntegerType.get()))))),
            optional(
                14,
                "b1",
                ListType.ofOptional(
                    15,
                    MapType.ofOptional(
                        16, 17, StringType.get(), ListType.ofOptional(18, FloatType.get()))))),
        "t:(b:{(i:int,s:chararray)}),m1:[(b:{(bytearray)},m2:[int])],b1:{([{(float)}])}",
        "");
  }

  @Test
  public void mapConversions() throws IOException {
    // consistent behavior for maps conversions. The below test case, correctly does not specify map
    // key types
    convertToPigSchema(
        new Schema(
            required(
                1,
                "a",
                MapType.ofRequired(
                    2,
                    3,
                    StringType.get(),
                    ListType.ofRequired(
                        4,
                        StructType.of(
                            required(5, "b", LongType.get()),
                            required(6, "c", StringType.get())))))),
        "a:[{(b:long,c:chararray)}]",
        "We do not specify the map key type here");
    // struct<a:map<string,map<string,double>>> -> (a:[[double]])
    // As per https://pig.apache.org/docs/latest/basic.html#map-schema. It seems that
    // we  only need to specify value type as keys are always of type chararray
    convertToPigSchema(
        new Schema(
            StructType.of(
                    required(
                        1,
                        "a",
                        MapType.ofRequired(
                            2,
                            3,
                            StringType.get(),
                            MapType.ofRequired(4, 5, StringType.get(), DoubleType.get()))))
                .fields()),
        "a:[[double]]",
        "A map key type does not need to be specified");
  }

  @Test
  public void testTupleInMap() throws IOException {
    Schema icebergSchema =
        new Schema(
            optional(
                1,
                "nested_list",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    ListType.ofOptional(
                        4,
                        StructType.of(
                            required(5, "id", LongType.get()),
                            optional(6, "data", StringType.get()))))));

    ResourceSchema pigSchema = SchemaUtil.convert(icebergSchema);
    // The output should contain a nested struct within a list within a map, I think.
    assertEquals("nested_list:[{(id:long,data:chararray)}]", pigSchema.toString());
  }

  @Test
  public void testLongInBag() throws IOException {
    Schema icebergSchema =
        new Schema(
            optional(
                1,
                "nested_list",
                MapType.ofOptional(
                    2, 3, StringType.get(), ListType.ofRequired(5, LongType.get()))));
    SchemaUtil.convert(icebergSchema);
  }

  @Test
  public void doubleWrappingTuples() throws IOException {
    // struct<a:array<struct<b:string>>> -> (a:{(b:chararray)})
    convertToPigSchema(
        new Schema(
            StructType.of(
                    required(
                        1,
                        "a",
                        ListType.ofRequired(2, StructType.of(required(3, "b", StringType.get())))))
                .fields()),
        "a:{(b:chararray)}",
        "A tuple inside a bag should not be double wrapped");
    // struct<a:array<boolean>> -> "(a:{(boolean)})
    convertToPigSchema(
        new Schema(
            StructType.of(required(1, "a", ListType.ofRequired(2, BooleanType.get()))).fields()),
        "a:{(boolean)}",
        "boolean (or anything non-tuple) element inside a bag should be wrapped inside a tuple");
  }

  private static void convertToPigSchema(
      Schema icebergSchema, String expectedPigSchema, String assertMessage) throws IOException {
    ResourceSchema pigSchema = SchemaUtil.convert(icebergSchema);
    assertEquals(assertMessage, expectedPigSchema, pigSchema.toString());
  }
}
