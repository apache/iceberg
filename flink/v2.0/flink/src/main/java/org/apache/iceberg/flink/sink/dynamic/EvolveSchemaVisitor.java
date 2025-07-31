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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Visitor class that accumulates the set of changes needed to evolve an existing schema into the
 * target schema. Changes are applied to an {@link UpdateSchema} operation.
 *
 * <p>We support:
 *
 * <ul>
 *   <li>Adding new columns
 *   <li>Widening the type of existing columsn
 *   <li>Reordering columns
 * </ul>
 *
 * We don't support:
 *
 * <ul>
 *   <li>Dropping columns
 *   <li>Renaming columns
 * </ul>
 *
 * The reason is that dropping columns would create issues with late / out of order data. Once we
 * drop fields, we wouldn't be able to easily add them back later without losing the associated
 * data. Renaming columns is not supported because we compare schemas by name, which doesn't allow
 * for renaming without additional hints.
 */
public class EvolveSchemaVisitor extends SchemaWithPartnerVisitor<Integer, Boolean> {

  private final UpdateSchema api;
  private final Schema existingSchema;
  private final Schema targetSchema;

  private EvolveSchemaVisitor(UpdateSchema api, Schema existingSchema, Schema targetSchema) {
    this.api = api;
    this.existingSchema = existingSchema;
    this.targetSchema = targetSchema;
  }

  /**
   * Adds changes needed to produce the target schema to an {@link UpdateSchema} operation.
   *
   * <p>Changes are accumulated to evolve the existingSchema into a targetSchema.
   *
   * @param api an UpdateSchema for adding changes
   * @param existingSchema an existing schema
   * @param targetSchema a new schema to compare with the existing
   */
  public static void visit(UpdateSchema api, Schema existingSchema, Schema targetSchema) {
    visit(
        targetSchema,
        -1,
        new EvolveSchemaVisitor(api, existingSchema, targetSchema),
        new CompareSchemasVisitor.PartnerIdByNameAccessors(existingSchema));
  }

  @Override
  public Boolean struct(Types.StructType struct, Integer partnerId, List<Boolean> existingFields) {
    if (partnerId == null) {
      return true;
    }

    // Add, update and order fields in the struct
    Types.StructType partnerStruct = findFieldType(partnerId).asStructType();
    String after = null;
    for (Types.NestedField targetField : struct.fields()) {
      Types.NestedField nestedField = partnerStruct.field(targetField.name());
      final String columnName;
      if (nestedField != null) {
        updateColumn(nestedField, targetField);
        columnName = this.existingSchema.findColumnName(nestedField.fieldId());
      } else {
        addColumn(partnerId, targetField);
        columnName = this.targetSchema.findColumnName(targetField.fieldId());
      }

      setPosition(columnName, after);
      after = columnName;
    }

    // Ensure that unused fields are made optional
    for (Types.NestedField existingField : partnerStruct.fields()) {
      if (struct.field(existingField.name()) == null) {
        if (existingField.isRequired()) {
          this.api.makeColumnOptional(this.existingSchema.findColumnName(existingField.fieldId()));
        }
      }
    }

    return false;
  }

  @Override
  public Boolean field(Types.NestedField field, Integer partnerId, Boolean isFieldMissing) {
    return partnerId == null;
  }

  @Override
  public Boolean list(Types.ListType list, Integer partnerId, Boolean isElementMissing) {
    if (partnerId == null) {
      return true;
    }

    Preconditions.checkState(
        !isElementMissing, "Error traversing schemas: element is missing, but list is present");

    Types.ListType partnerList = findFieldType(partnerId).asListType();
    updateColumn(partnerList.fields().get(0), list.fields().get(0));

    return false;
  }

  @Override
  public Boolean map(
      Types.MapType map, Integer partnerId, Boolean isKeyMissing, Boolean isValueMissing) {
    if (partnerId == null) {
      return true;
    }

    Preconditions.checkState(
        !isKeyMissing, "Error traversing schemas: key is missing, but map is present");
    Preconditions.checkState(
        !isValueMissing, "Error traversing schemas: value is missing, but map is present");

    Types.MapType partnerMap = findFieldType(partnerId).asMapType();
    updateColumn(partnerMap.fields().get(0), map.fields().get(0));
    updateColumn(partnerMap.fields().get(1), map.fields().get(1));

    return false;
  }

  @Override
  public Boolean primitive(Type.PrimitiveType primitive, Integer partnerId) {
    return partnerId == null;
  }

  private Type findFieldType(int fieldId) {
    if (fieldId == -1) {
      return existingSchema.asStruct();
    } else {
      return existingSchema.findField(fieldId).type();
    }
  }

  private void addColumn(int parentId, Types.NestedField field) {
    String parentName = existingSchema.findColumnName(parentId);
    api.addColumn(parentName, field.name(), field.type(), field.doc());
  }

  private void updateColumn(Types.NestedField existingField, Types.NestedField targetField) {
    String existingColumnName = this.existingSchema.findColumnName(existingField.fieldId());

    boolean needsOptionalUpdate = targetField.isOptional() && existingField.isRequired();
    boolean needsTypeUpdate =
        targetField.type().isPrimitiveType() && !targetField.type().equals(existingField.type());
    boolean needsDocUpdate =
        targetField.doc() != null && !targetField.doc().equals(existingField.doc());

    if (needsOptionalUpdate) {
      api.makeColumnOptional(existingColumnName);
    }

    if (needsTypeUpdate) {
      api.updateColumn(existingColumnName, targetField.type().asPrimitiveType());
    }

    if (needsDocUpdate) {
      api.updateColumnDoc(existingColumnName, targetField.doc());
    }
  }

  private void setPosition(String columnName, String after) {
    if (after == null) {
      this.api.moveFirst(columnName);
    } else {
      this.api.moveAfter(columnName, after);
    }
  }
}
