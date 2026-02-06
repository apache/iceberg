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
package org.apache.iceberg;

import org.apache.avro.generic.IndexedRecord;

/** IndexedRecord implementation to wrap a StructLike for writing to Avro. */
class IndexedStructLike implements StructLike, IndexedRecord {
  private final org.apache.avro.Schema avroSchema;
  private StructLike wrapped = null;

  IndexedStructLike(org.apache.avro.Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  IndexedStructLike wrap(StructLike struct) {
    this.wrapped = struct;
    return this;
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return wrapped.get(pos, javaClass);
  }

  @Override
  public Object get(int pos) {
    return get(pos, Object.class);
  }

  @Override
  public <T> void set(int pos, T value) {
    wrapped.set(pos, value);
  }

  @Override
  public void put(int pos, Object value) {
    set(pos, value);
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return avroSchema;
  }
}
