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
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

/**
 * Row key of a table.
 * <p>
 * Row key is a definition of table row uniqueness,
 * similar to the concept of primary key in a relational database system.
 * A row should be unique in a table based on the values of an unordered set of {@link RowKeyIdentifierField}.
 * Iceberg itself does not enforce row uniqueness based on this key.
 * It is leveraged by operations such as streaming upsert.
 */
public class RowKey implements Serializable {

  private static final RowKey NOT_IDENTIFIED = new RowKey(new Schema(), Sets.newHashSet());

  private final Schema schema;
  private final RowKeyIdentifierField[] identifierFields;

  private transient volatile Set<RowKeyIdentifierField> identifierFieldSet;

  private RowKey(Schema schema, Set<RowKeyIdentifierField> identifierFields) {
    this.schema = schema;
    this.identifierFields = identifierFields.toArray(new RowKeyIdentifierField[0]);
  }

  /**
   * Returns the {@link Schema} referenced by the row key
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Return the set of {@link RowKeyIdentifierField} in the row key
   * <p>
   * @return the set of fields in the row key
   */
  public Set<RowKeyIdentifierField> identifierFields() {
    return lazyIdentifierFieldSet();
  }

  private Set<RowKeyIdentifierField> lazyIdentifierFieldSet() {
    if (identifierFieldSet == null) {
      synchronized (this) {
        if (identifierFieldSet == null) {
          identifierFieldSet = ImmutableSet.copyOf(identifierFields);
        }
      }
    }

    return identifierFieldSet;
  }

  /**
   * Returns the default row key that has no field
   */
  public static RowKey notIdentified() {
    return NOT_IDENTIFIED;
  }

  /**
   * Returns true if the row key is the default one with no field
   */
  public boolean isNotIdentified() {
    return identifierFields.length < 1;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    RowKey that = (RowKey) other;
    return identifierFields().equals(that.identifierFields());
  }

  @Override
  public int hashCode() {
    return identifierFields().hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (RowKeyIdentifierField field : identifierFields) {
      sb.append("\n");
      sb.append("  ").append(field);
    }
    if (identifierFields.length > 0) {
      sb.append("\n");
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Creates a new {@link Builder row key builder} for the given {@link Schema}.
   *
   * @param schema a schema
   * @return a row key builder for the given schema.
   */
  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  /**
   * A builder to create valid {@link RowKey row key}.
   * <p>
   * Call {@link #builderFor(Schema)} to create a new builder.
   */
  public static class Builder {
    private final Schema schema;
    private final Set<RowKeyIdentifierField> fields = Sets.newHashSet();

    private Builder(Schema schema) {
      this.schema = schema;
    }

    public Builder addField(String name) {
      Types.NestedField column = schema.findField(name);
      ValidationException.check(column != null, "Cannot find column with name %s in schema %s", name, schema);
      return addField(column);
    }

    public Builder addField(int id) {
      Types.NestedField column = schema.findField(id);
      ValidationException.check(column != null, "Cannot find column with ID %s in schema %s", id, schema);
      return addField(column);
    }

    private Builder addField(Types.NestedField column) {
      ValidationException.check(column.isRequired(),
          "Cannot add column %s to row key because it is not a required column in schema %s", column, schema);
      ValidationException.check(column.type().isPrimitiveType(),
          "Cannot add column %s to row key because it is not a primitive data type in schema %s", column, schema);
      fields.add(new RowKeyIdentifierField(column.fieldId()));
      return this;
    }

    public RowKey build() {
      if (fields.size() == 0) {
        return NOT_IDENTIFIED;
      }

      return new RowKey(schema, fields);
    }
  }
}
