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
import org.apache.iceberg.types.Types;

/**
 * A reference to a field by ID rather than name. This extends NamedReference because it is a more
 * specific in that the name has already been resolved to a field ID. Because the name has been
 * resolved, the name is informational.
 */
public class IDReference<T> extends NamedReference<T> {
  private final int id;

  public IDReference(String name, int id) {
    super(name);
    this.id = id;
  }

  public int id() {
    return id;
  }

  @Override
  public BoundReference<T> bind(Types.StructType struct, boolean caseSensitive) {
    Schema schema = struct.asSchema();
    Types.NestedField field = schema.findField(id);
    ValidationException.check(
        field != null, "Cannot find field by id %s in struct: %s", id, schema.asStruct());

    return new BoundReference<>(field, schema.accessorForField(field.fieldId()), name());
  }

  @Override
  public String toString() {
    return String.format("ref(name=\"%s\", id=\"%s\")", name(), id);
  }
}
