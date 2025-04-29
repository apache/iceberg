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

public class BoundExtract<T> implements BoundTerm<T> {
  private final BoundReference<?> ref;
  private final String path;
  private final Type type;

  BoundExtract(BoundReference<?> ref, String path, Type type) {
    this.ref = ref;
    this.path = PathUtil.toNormalizedPath(PathUtil.parse(path));
    this.type = type;
  }

  @Override
  public BoundReference<?> ref() {
    return ref;
  }

  public String path() {
    return path;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public boolean isEquivalentTo(BoundTerm<?> other) {
    if (other instanceof BoundExtract) {
      BoundExtract<?> that = (BoundExtract<?>) other;
      return ref.isEquivalentTo(that.ref) && path.equals(that.path) && type.equals(that.type);
    }

    return false;
  }

  @Override
  public T eval(StructLike struct) {
    throw new UnsupportedOperationException("Cannot evaluate " + this);
  }

  @Override
  public String toString() {
    return "extract(" + ref + ", path=" + path + ", type=" + type + ")";
  }
}
