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
package org.apache.iceberg.util;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class Pair<X, Y> implements IndexedRecord, SpecificData.SchemaConstructable, Serializable {
  public static <X, Y> Pair<X, Y> of(X first, Y second) {
    return new Pair<>(first, second);
  }

  private static final LoadingCache<Pair<Class<?>, Class<?>>, Schema> SCHEMA_CACHE =
      Caffeine.newBuilder()
          .build(
              new CacheLoader<Pair<Class<?>, Class<?>>, Schema>() {
                @Override
                @SuppressWarnings("deprecation")
                public Schema load(Pair<Class<?>, Class<?>> key) {
                  Schema xSchema = ReflectData.get().getSchema(key.first);
                  Schema ySchema = ReflectData.get().getSchema(key.second);
                  return Schema.createRecord(
                      "pair",
                      null,
                      null,
                      false,
                      Lists.newArrayList(
                          new Schema.Field("x", xSchema, null, null),
                          new Schema.Field("y", ySchema, null, null)));
                }
              });

  private Schema schema = null;
  private X first;
  private Y second;

  /** Constructor used by Avro */
  private Pair(Schema schema) {
    this.schema = schema;
  }

  private Pair(X first, Y second) {
    this.first = first;
    this.second = second;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    if (i == 0) {
      this.first = (X) v;
      return;
    } else if (i == 1) {
      this.second = (Y) v;
      return;
    }
    throw new IllegalArgumentException("Cannot set value " + i + " (not 0 or 1): " + v);
  }

  @Override
  public Object get(int i) {
    if (i == 0) {
      return first;
    } else if (i == 1) {
      return second;
    }
    throw new IllegalArgumentException("Cannot get value " + i + " (not 0 or 1)");
  }

  @Override
  public Schema getSchema() {
    if (schema == null) {
      this.schema = SCHEMA_CACHE.get(Pair.of(first.getClass(), second.getClass()));
    }
    return schema;
  }

  public X first() {
    return first;
  }

  public Y second() {
    return second;
  }

  @Override
  public String toString() {
    return "(" + first + ", " + second + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(first, second);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof Pair)) {
      return false;
    }
    Pair<?, ?> otherPair = (Pair<?, ?>) other;
    return Objects.equal(first, otherPair.first) && Objects.equal(second, otherPair.second);
  }
}
