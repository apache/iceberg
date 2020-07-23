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

package org.apache.iceberg.avro;

import java.util.Deque;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * A abstract avro schema visitor with partner type. This class is for both reading and writing:
 * - For reading, the avro schema could evolve into the partner type. (schema evolution)
 * - For writing, the avro schema should be consistent with partner type.
 *
 * @param <P> Partner type.
 * @param <T> Return T.
 */
public abstract class AvroWithPartnerSchemaVisitor<P, T> {

  public static <P, T> T visit(P partner, Schema schema, AvroWithPartnerSchemaVisitor<P, T> visitor) {
    switch (schema.getType()) {
      case RECORD:
        return visitRecord(partner, schema, visitor);

      case UNION:
        return visitUnion(partner, schema, visitor);

      case ARRAY:
        return visitArray(partner, schema, visitor);

      case MAP:
        P keyType = visitor.mapKeyType(partner);
        Preconditions.checkArgument(
            visitor.isValidMapKey(keyType),
            "Invalid map: %s is not a string", keyType);
        return visitor.map(partner, schema, visit(visitor.mapValueType(partner), schema.getValueType(), visitor));

      default:
        return visitor.primitive(partner, schema);
    }
  }

  // ---------------------------------- Static helpers ---------------------------------------------

  private static <P, T> T visitRecord(P struct, Schema record, AvroWithPartnerSchemaVisitor<P, T> visitor) {
    // check to make sure this hasn't been visited before
    String name = record.getFullName();
    Preconditions.checkState(!visitor.recordLevels.contains(name),
        "Cannot process recursive Avro record %s", name);
    List<Schema.Field> fields = record.getFields();
    visitor.recordLevels.push(name);

    List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());

    if (visitor.schemaEvolution()) {
      for (Schema.Field field : fields) {
        int fieldId = AvroSchemaUtil.getFieldId(field);
        names.add(field.name());
        results.add(visit(visitor.structFieldTypeById(struct, fieldId), field.schema(), visitor));
      }
    } else {
      String[] fieldNames = visitor.structFieldNames(struct);
      P[] fieldTypes = visitor.structFieldTypes(struct);
      Preconditions.checkArgument(fieldTypes.length == fields.size(),
          "Structs do not match: %s != %s", struct, record);
      for (int i = 0; i < fieldTypes.length; i += 1) {
        String fieldName = fieldNames[i];
        Schema.Field field = fields.get(i);
        Preconditions.checkArgument(AvroSchemaUtil.makeCompatibleName(fieldName).equals(field.name()),
            "Structs do not match: field %s != %s", fieldName, field.name());
        results.add(visit(fieldTypes[i], field.schema(), visitor));
      }
    }

    visitor.recordLevels.pop();

    return visitor.record(struct, record, names, results);
  }

  private static <P, T> T visitUnion(P type, Schema union, AvroWithPartnerSchemaVisitor<P, T> visitor) {
    List<Schema> types = union.getTypes();
    Preconditions.checkArgument(AvroSchemaUtil.isOptionSchema(union),
        "Cannot visit non-option union: %s", union);
    List<T> options = Lists.newArrayListWithExpectedSize(types.size());
    for (Schema branch : types) {
      if (branch.getType() == Schema.Type.NULL) {
        options.add(visit(visitor.nullType(), branch, visitor));
      } else {
        options.add(visit(type, branch, visitor));
      }
    }
    return visitor.union(type, union, options);
  }

  private static <P, T> T visitArray(P type, Schema array, AvroWithPartnerSchemaVisitor<P, T> visitor) {
    if (array.getLogicalType() instanceof LogicalMap || visitor.isMapType(type)) {
      Preconditions.checkState(
          AvroSchemaUtil.isKeyValueSchema(array.getElementType()),
          "Cannot visit invalid logical map type: %s", array);
      List<Schema.Field> keyValueFields = array.getElementType().getFields();
      return visitor.map(type, array,
          visit(visitor.mapKeyType(type), keyValueFields.get(0).schema(), visitor),
          visit(visitor.mapValueType(type), keyValueFields.get(1).schema(), visitor));

    } else {
      return visitor.array(type, array, visit(visitor.arrayElementType(type), array.getElementType(), visitor));
    }
  }

  /**
   * Just for checking state.
   */
  private Deque<String> recordLevels = Lists.newLinkedList();

  // ---------------------------------- Partner type methods ---------------------------------------------

  public boolean schemaEvolution() {
    return false;
  }

  public abstract boolean isMapType(P type);

  public abstract boolean isValidMapKey(P type);

  public abstract P arrayElementType(P arrayType);

  public abstract P mapKeyType(P mapType);
  public abstract P mapValueType(P mapType);

  public String[] structFieldNames(P structType) {
    throw new UnsupportedOperationException();
  }

  public P[] structFieldTypes(P structType) {
    throw new UnsupportedOperationException();
  }

  public P structFieldTypeById(P structType, int id) {
    throw new UnsupportedOperationException();
  }

  public abstract P nullType();

  // ---------------------------------- Type visitors ---------------------------------------------

  public T record(P struct, Schema record, List<String> names, List<T> fields) {
    return null;
  }

  public T union(P type, Schema union, List<T> options) {
    return null;
  }

  public T array(P sArray, Schema array, T element) {
    return null;
  }

  public T map(P sMap, Schema map, T key, T value) {
    return null;
  }

  public T map(P sMap, Schema map, T value) {
    return null;
  }

  public T primitive(P type, Schema primitive) {
    return null;
  }
}
