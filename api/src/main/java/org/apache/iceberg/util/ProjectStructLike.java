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

import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;

public class ProjectStructLike implements StructLike {
  public static ProjectStructLike of(Schema schema, int... fieldIds) {
    return new ProjectStructLike(IntStream.of(fieldIds)
        .mapToObj(schema::accessorForField)
        .toArray((IntFunction<Accessor<StructLike>[]>) Accessor[]::new));
  }

  private final Accessor<StructLike>[] accessors;
  private StructLike struct = null;

  public ProjectStructLike(Accessor<StructLike>[] accessors) {
    this.accessors = accessors;
  }

  public ProjectStructLike wrap(StructLike newStruct) {
    this.struct = newStruct;
    return this;
  }

  @Override
  public int size() {
    return accessors.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(accessors[pos].get(struct));
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Cannot set fields through a projection");
  }
}
