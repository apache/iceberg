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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

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
    Assertions.assertThat(actual.get(1)).isEqualTo("`long`");
    Assertions.assertThat(actual.get(2)).isEqualTo("`struct`");
    Assertions.assertThat(actual.get(3)).isEqualTo("`struct`.`long`");
    Assertions.assertThat(actual.get(4)).isEqualTo("`listOfLongs`");
    Assertions.assertThat(actual.get(5)).isEqualTo("`listOfLongs`.`_elem`");
    Assertions.assertThat(actual.get(6)).isEqualTo("`listOfStructs`");
    Assertions.assertThat(actual.get(7)).isEqualTo("`listOfStructs`.`_elem`");
    Assertions.assertThat(actual.get(8)).isEqualTo("`listOfStructs`.`_elem`.`long`");
    Assertions.assertThat(actual.get(9)).isEqualTo("`map`");
    Assertions.assertThat(actual.get(10)).isEqualTo("`map`.`_key`");
    Assertions.assertThat(actual.get(11)).isEqualTo("`map`.`_value`");
    Assertions.assertThat(actual.get(12)).isEqualTo("`mapOfStructs`");
    Assertions.assertThat(actual.get(13)).isEqualTo("`mapOfStructs`.`_key`");
    Assertions.assertThat(actual.get(14)).isEqualTo("`mapOfStructs`.`_value`");
    Assertions.assertThat(actual.get(15)).isEqualTo("`mapOfStructs`.`_key`.`long`");
    Assertions.assertThat(actual.get(16)).isEqualTo("`mapOfStructs`.`_value`.`long`");
    Assertions.assertThat(actual.get(17)).isEqualTo("`listOfMapsOfStruct`");
    Assertions.assertThat(actual.get(18)).isEqualTo("`listOfMapsOfStruct`.`_elem`");
    Assertions.assertThat(actual.get(19)).isEqualTo("`listOfMapsOfStruct`.`_elem`.`_key`");
    Assertions.assertThat(actual.get(20)).isEqualTo("`listOfMapsOfStruct`.`_elem`.`_value`");
    Assertions.assertThat(actual.get(21)).isEqualTo("`listOfMapsOfStruct`.`_elem`.`_key`.`long`");
    Assertions.assertThat(actual.get(22)).isEqualTo("`listOfMapsOfStruct`.`_elem`.`_value`.`long`");
    Assertions.assertThat(actual.get(23)).isEqualTo("`col.with.dots`");
    Assertions.assertThat(actual.get(24)).isEqualTo("`col.with.dots`.`inner.col.with.dots`");
    Assertions.assertThat(actual.get(25)).isEqualTo("`colW!th$peci@lCh@rs`");
    Assertions.assertThat(actual.get(26)).isEqualTo("`colWith``Quotes```");
  }
}
