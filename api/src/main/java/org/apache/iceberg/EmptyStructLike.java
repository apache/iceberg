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

import java.io.Serializable;

class EmptyStructLike implements StructLike, Serializable {

  private static final EmptyStructLike INSTANCE = new EmptyStructLike();

  private EmptyStructLike() {}

  static EmptyStructLike get() {
    return INSTANCE;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    throw new UnsupportedOperationException("Can't retrieve values from an empty struct");
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Can't modify an empty struct");
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    return other != null && getClass() == other.getClass();
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return "StructLike{}";
  }
}
