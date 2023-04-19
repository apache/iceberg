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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestIdToOrcName {

  @Test
  public void testIdToQuotedColumnName() {
    Schema schema =
        new Schema(
            required(1, "long", Types.LongType.get()),
            required(2, "struct", Types.StructType.of(required(3, "long", Types.LongType.get()))),
            required(4, "listOfLongs", Types.ListType.ofRequired(5, Types.LongType.get())),
            required(
                6,
                "listOfStructs",
                Types.ListType.ofRequired(
                    7, Types.StructType.of(required(8, "long", Types.LongType.get())))),
            required(
                9,
                "map",
                Types.MapType.ofRequired(10, 11, Types.LongType.get(), Types.LongType.get())),
            required(
                12,
                "mapOfStructs",
                Types.MapType.ofRequired(
                    13,
                    14,
                    Types.StructType.of(required(15, "long", Types.LongType.get())),
                    Types.StructType.of(required(16, "long", Types.LongType.get())))),
            required(
                17,
                "listOfMapsOfStruct",
                Types.ListType.ofRequired(
                    18,
                    Types.MapType.ofRequired(
                        19,
                        20,
                        Types.StructType.of(required(21, "long", Types.LongType.get())),
                        Types.StructType.of(required(22, "long", Types.LongType.get()))))),
            required(
                23,
                "col.with.dots",
                Types.StructType.of(required(24, "inner.col.with.dots", Types.LongType.get()))),
            required(25, "colW!th$peci@lCh@rs", Types.LongType.get()),
            required(26, "colWith`Quotes`", Types.LongType.get()));

    Map<Integer, String> actual = ORCSchemaUtil.idToOrcName(schema);
    Assert.assertEquals("`long`", actual.get(1));
    Assert.assertEquals("`struct`", actual.get(2));
    Assert.assertEquals("`struct`.`long`", actual.get(3));
    Assert.assertEquals("`listOfLongs`", actual.get(4));
    Assert.assertEquals("`listOfLongs`.`_elem`", actual.get(5));
    Assert.assertEquals("`listOfStructs`", actual.get(6));
    Assert.assertEquals("`listOfStructs`.`_elem`", actual.get(7));
    Assert.assertEquals("`listOfStructs`.`_elem`.`long`", actual.get(8));
    Assert.assertEquals("`map`", actual.get(9));
    Assert.assertEquals("`map`.`_key`", actual.get(10));
    Assert.assertEquals("`map`.`_value`", actual.get(11));
    Assert.assertEquals("`mapOfStructs`", actual.get(12));
    Assert.assertEquals("`mapOfStructs`.`_key`", actual.get(13));
    Assert.assertEquals("`mapOfStructs`.`_value`", actual.get(14));
    Assert.assertEquals("`mapOfStructs`.`_key`.`long`", actual.get(15));
    Assert.assertEquals("`mapOfStructs`.`_value`.`long`", actual.get(16));
    Assert.assertEquals("`listOfMapsOfStruct`", actual.get(17));
    Assert.assertEquals("`listOfMapsOfStruct`.`_elem`", actual.get(18));
    Assert.assertEquals("`listOfMapsOfStruct`.`_elem`.`_key`", actual.get(19));
    Assert.assertEquals("`listOfMapsOfStruct`.`_elem`.`_value`", actual.get(20));
    Assert.assertEquals("`listOfMapsOfStruct`.`_elem`.`_key`.`long`", actual.get(21));
    Assert.assertEquals("`listOfMapsOfStruct`.`_elem`.`_value`.`long`", actual.get(22));
    Assert.assertEquals("`col.with.dots`", actual.get(23));
    Assert.assertEquals("`col.with.dots`.`inner.col.with.dots`", actual.get(24));
    Assert.assertEquals("`colW!th$peci@lCh@rs`", actual.get(25));
    Assert.assertEquals("`colWith``Quotes```", actual.get(26));
  }
}
