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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class UnboundExtract<T> implements UnboundTerm<T> {
  private final NamedReference<?> ref;
  private final String path;
  private final Type.PrimitiveType type;

  public UnboundExtract(NamedReference<?> ref, String path, String type) {
    this.ref = ref;
    this.path = path;
    this.type = Types.fromPrimitiveString(type);
    // verify that the path is well-formed
    PathUtil.parse(path);
  }

  @Override
  public BoundTerm<T> bind(Types.StructType struct, boolean caseSensitive) {
    BoundReference<?> boundRef = ref.bind(struct, caseSensitive);
    ValidationException.check(
        Types.VariantType.get().equals(boundRef.type()),
        "Cannot bind extract, not a variant: %s",
        boundRef.name());
    ValidationException.check(
        !type.equals(Types.UnknownType.get()), "Invalid type to extract: unknown");
    return new BoundExtract<>(boundRef, path, type);
  }

  @Override
  public NamedReference<?> ref() {
    return ref;
  }

  public String path() {
    return path;
  }

  public Type type() {
    return type;
  }

  @Override
  public String toString() {
    return "extract(" + ref + ", path=" + path + ", type=" + type + ")";
  }
}
