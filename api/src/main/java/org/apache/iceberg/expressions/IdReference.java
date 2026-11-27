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
 * A reference to a field by field ID.
 *
 * <p>Binding resolves the field with this ID, so the reference is not affected by column renames
 * and produces the field's current type. Case sensitivity does not apply.
 *
 * @param <T> the Java type of values produced by the referenced field
 */
public class IdReference<T> implements ValueExpression<T>, Unbound<T, BoundReference<T>> {
  private final int fieldId;

  IdReference(int fieldId) {
    this.fieldId = fieldId;
  }

  public int fieldId() {
    return fieldId;
  }

  @Override
  public BoundReference<T> bind(Types.StructType struct, boolean caseSensitive) {
    Schema schema = struct.asSchema();
    Types.NestedField field = schema.findField(fieldId);
    ValidationException.check(
        field != null, "Cannot find field with ID %s in struct: %s", fieldId, struct);
    return new BoundReference<>(
        field, schema.accessorForField(fieldId), schema.findColumnName(fieldId));
  }

  @Override
  public String toString() {
    return String.format("ref(id=%s)", fieldId);
  }
}
