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

import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

public class NamedReference<T> implements UnboundTerm<T>, Reference<T> {
  private final String name;

  NamedReference(String name) {
    Preconditions.checkNotNull(name, "Name cannot be null");
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public BoundReference<T> bind(Types.StructType struct, boolean caseSensitive) {
    Schema schema = struct.asSchema();
    Types.NestedField field =
        caseSensitive ? schema.findField(name) : schema.caseInsensitiveFindField(name);

    ValidationException.check(
        field != null, "Cannot find field '%s' in struct: %s", name, schema.asStruct());

    return new BoundReference<>(field, schema.accessorForField(field.fieldId()), name);
  }

  @Override
  public NamedReference<T> ref() {
    return this;
  }

  @Override
  public String toString() {
    return String.format("ref(name=\"%s\")", name);
  }
}
