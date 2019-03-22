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

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;

public class Pair<X, Y> implements IndexedRecord, SpecificData.SchemaConstructable, Serializable {
  public static <X, Y> Pair<X, Y> of(X x, Y y) {
    return new Pair<>(x, y);
  }

  private static final LoadingCache<Pair<Class<?>, Class<?>>, Schema> SCHEMA_CACHE = CacheBuilder
      .newBuilder()
      .build(new CacheLoader<Pair<Class<?>, Class<?>>, Schema>() {
        @Override
        @SuppressWarnings("deprecation")
        public Schema load(Pair<Class<?>, Class<?>> key) {
          Schema xSchema = ReflectData.get().getSchema(key.x);
          Schema ySchema = ReflectData.get().getSchema(key.y);
          return Schema.createRecord("pair", null, null, false, Lists.newArrayList(
              new Schema.Field("x", xSchema, null, null),
              new Schema.Field("y", ySchema, null, null)
          ));
        }
      });

  private Schema schema = null;
  private X x;
  private Y y;

  /**
   * Constructor used by Avro
   */
  private Pair(Schema schema) {
    this.schema = schema;
  }

  private Pair(X x, Y y) {
    this.x = x;
    this.y = y;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    if (i == 0) {
      this.x = (X) v;
      return;
    } else if (i == 1) {
      this.y = (Y) v;
      return;
    }
    throw new IllegalArgumentException("Cannot set value " + i + " (not 0 or 1): " + v);
  }

  @Override
  public Object get(int i) {
    if (i == 0) {
      return x;
    } else if (i == 1) {
      return y;
    }
    throw new IllegalArgumentException("Cannot get value " + i + " (not 0 or 1)");
  }

  @Override
  public Schema getSchema() {
    if (schema == null) {
      this.schema = SCHEMA_CACHE.getUnchecked(Pair.of(x.getClass(), y.getClass()));
    }
    return schema;
  }

  public X first() {
    return x;
  }

  public Y second() {
    return y;
  }

  @Override
  public String toString() {
    return "(" + String.valueOf(x) + ", " + String.valueOf(y) + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(x, y);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (getClass() != other.getClass()) {
      return false;
    }
    Pair<?, ?> otherPair = (Pair<?, ?>) other;
    return Objects.equal(x, otherPair.x) && Objects.equal(y, otherPair.y);
  }
}
