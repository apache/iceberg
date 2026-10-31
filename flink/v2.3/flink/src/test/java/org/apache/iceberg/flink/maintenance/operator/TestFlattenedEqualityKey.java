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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestFlattenedEqualityKey {

  private static final Types.StructType USER_TYPE =
      Types.StructType.of(
          Types.NestedField.required(3, "id", Types.IntegerType.get()),
          Types.NestedField.optional(4, "name", Types.StringType.get()));

  private static final Types.StructType NESTED_KEY =
      Types.StructType.of(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "user", USER_TYPE));

  @Test
  void topLevelKeyIsUnchanged() {
    Types.StructType keyType =
        Types.StructType.of(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    GenericRecord key = GenericRecord.create(keyType);
    key.setField("id", 7);
    key.setField("data", "a");

    FlattenedEqualityKey flat = new FlattenedEqualityKey(keyType).wrap(key);

    assertThat(flat.flatType()).isEqualTo(keyType);
    assertThat(flat.get(0, Integer.class)).isEqualTo(7);
    assertThat(flat.get(1, String.class)).isEqualTo("a");
  }

  @Test
  void flattensNestedFieldsDepthFirst() {
    GenericRecord user = GenericRecord.create(USER_TYPE);
    user.setField("id", 10);
    user.setField("name", "n");
    GenericRecord key = GenericRecord.create(NESTED_KEY);
    key.setField("id", 1);
    key.setField("user", user);

    FlattenedEqualityKey flat = new FlattenedEqualityKey(NESTED_KEY).wrap(key);

    assertThat(flat.flatType().fields())
        .extracting(Types.NestedField::fieldId)
        .containsExactly(1, 3, 4);
    assertThat(flat.size()).isEqualTo(3);
    assertThat(flat.get(0, Integer.class)).isEqualTo(1);
    assertThat(flat.get(1, Integer.class)).isEqualTo(10);
    assertThat(flat.get(2, String.class)).isEqualTo("n");
  }

  @Test
  void leafOfNullStructIsNull() {
    GenericRecord key = GenericRecord.create(NESTED_KEY);
    key.setField("id", 1);

    FlattenedEqualityKey flat = new FlattenedEqualityKey(NESTED_KEY).wrap(key);

    assertThat(flat.get(1, Integer.class)).isNull();
    assertThat(flat.get(2, String.class)).isNull();
  }

  @Test
  void rejectsFieldNestedInList() {
    Types.StructType keyType =
        Types.StructType.of(
            Types.NestedField.optional(
                5, "tags", Types.ListType.ofRequired(6, Types.StringType.get())));

    assertThatThrownBy(() -> new FlattenedEqualityKey(keyType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Equality field tags of type list<string> is not supported");
  }
}
