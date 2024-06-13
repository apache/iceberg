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
package org.apache.iceberg.io;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestStructCopy {

  @Test
  void copy() {
    StructLikeTest inner = new StructLikeTest();
    inner.set(0, "TheInner");

    Types.NestedField f1 = Types.NestedField.required(1, "field1", Types.BooleanType.get());
    Schema schema = new Schema(Arrays.asList(f1), Collections.emptyMap());

    StructLikeTest s1 = new StructLikeTest();
    s1.set(0, "Hello");
    s1.set(1, 12);
    s1.set(2, inner);
    s1.set(3, "bytearray".getBytes(StandardCharsets.UTF_8));
    s1.set(4, schema);
    s1.set(5, new int[] {2023, 2024, 2025});
    s1.set(6, new Float[] {20.1F, 30.2F});

    // do the copy
    StructLike copy = StructCopy.copy(s1);

    // change original
    inner.set(0, "other");
    s1.set(0, "By");
    s1.set(1, 7);
    s1.set(2, inner);
    s1.set(3, "final".getBytes(StandardCharsets.UTF_8));

    Types.NestedField f2 = Types.NestedField.required(1, "f2", Types.LongType.get());
    Schema schema2 = new Schema(Arrays.asList(f2), Collections.emptyMap());
    s1.set(4, schema2);
    s1.set(5, new int[] {0});
    s1.set(6, new Float[] {0.1F});

    // type copy keep values
    Assertions.assertEquals(7, copy.size());
    Assertions.assertEquals("Hello", copy.get(0, String.class));
    Assertions.assertEquals(12, copy.get(1, Integer.class));
    Assertions.assertEquals("TheInner", copy.get(2, StructLike.class).get(0, String.class));
    Assertions.assertArrayEquals(
        "bytearray".getBytes(StandardCharsets.UTF_8), copy.get(3, byte[].class));
    Assertions.assertEquals(schema.columns(), copy.get(4, Schema.class).columns());
    Assertions.assertArrayEquals(new int[] {2023, 2024, 2025}, copy.get(5, int[].class));

    Float[] floats = copy.get(6, Float[].class);
    Assertions.assertEquals(2, floats.length);
    Assertions.assertTrue(Math.abs(floats[0] - 20.1f) < Float.MIN_VALUE);
  }

  static class StructLikeTest implements StructLike {

    static class FieldLike {
      private final Class<?> clazz;

      private final Function<StructLikeTest, Object> getter;

      private final BiConsumer<StructLikeTest, Object> setter;

      FieldLike(
          final Class<?> clazz,
          final Function<StructLikeTest, Object> getter,
          final BiConsumer<StructLikeTest, Object> setter) {
        this.clazz = clazz;
        this.getter = getter;
        this.setter = setter;
      }

      public <T> T get(StructLikeTest record, Class<T> javaClass) {
        if (javaClass.isAssignableFrom(clazz)) {
          return (T) getter.apply(record);
        }
        throw new RuntimeException(
            "getter issue with field of " + clazz.getName() + " with class " + javaClass.getName());
      }

      public void set(StructLikeTest record, Object value) {
        Class<?> javaClass = value.getClass();
        if (clazz.isAssignableFrom(javaClass)) {
          setter.accept(record, value);
        } else {
          throw new RuntimeException(
              "setter issue with field of "
                  + clazz.getName()
                  + " with class "
                  + javaClass.getName());
        }
      }
    }

    private static FieldLike[] fields =
        new FieldLike[] {
          new FieldLike(
              String.class,
              (StructLikeTest str) -> str.field1,
              (StructLikeTest str, Object value) -> str.field1 = (String) value),
          new FieldLike(
              Integer.class,
              (StructLikeTest str) -> str.field2,
              (StructLikeTest str, Object value) -> str.field2 = (Integer) value),
          new FieldLike(
              StructLike.class,
              (StructLikeTest str) -> str.field3,
              (StructLikeTest str, Object value) -> str.field3 = (StructLike) value),
          new FieldLike(
              byte[].class,
              (StructLikeTest str) -> str.field4,
              (StructLikeTest str, Object value) -> str.field4 = (byte[]) value),
          new FieldLike(
              Schema.class,
              (StructLikeTest str) -> str.field5,
              (StructLikeTest str, Object value) -> str.field5 = (Schema) value),
          new FieldLike(
              int[].class,
              (StructLikeTest str) -> str.field6,
              (StructLikeTest str, Object value) -> str.field6 = (int[]) value),
          new FieldLike(
              Float[].class,
              (StructLikeTest str) -> str.field7,
              (StructLikeTest str, Object value) -> str.field7 = (Float[]) value),
        };

    private String field1;

    private int field2;

    private StructLike field3;

    private byte[] field4;

    private Schema field5; // Serializable test.

    private int[] field6;

    private Float[] field7;

    @Override
    public int size() {
      return 7;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      if (pos >= 0 && pos < fields.length && javaClass.isAssignableFrom(fields[pos].clazz)) {
        return fields[pos].get(this, javaClass);
      }
      throw new RuntimeException("issue with field " + pos + " with class " + javaClass.getName());
    }

    @Override
    public <T> void set(int pos, T value) {
      if (value == null) {
        return;
      }
      Class<T> javaClass = (Class<T>) value.getClass();
      if (pos >= 0 && pos < fields.length) {
        fields[pos].set(this, value);
      } else {
        throw new RuntimeException(
            "issue with field " + pos + " with class " + javaClass.getName());
      }
    }
  }
}
