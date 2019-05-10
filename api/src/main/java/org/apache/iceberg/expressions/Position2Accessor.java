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

package org.apache.iceberg.expressions;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;

class Position2Accessor implements Accessor<StructLike> {
  private final int p0;
  private final int p1;
  private final Type type;
  private final Class<?> javaClass;

  Position2Accessor(int pos, PositionAccessor wrapped) {
    this.p0 = pos;
    this.p1 = wrapped.position();
    this.type = wrapped.type();
    this.javaClass = wrapped.javaClass();
  }

  @Override
  public Object get(StructLike row) {
    return row.get(p0, StructLike.class).get(p1, javaClass);
  }

  @Override
  public Type type() {
    return type;
  }

  public int p0() {
    return p0;
  }

  public int p1() {
    return p1;
  }

  public Class<?> javaClass() {
    return javaClass;
  }
}
