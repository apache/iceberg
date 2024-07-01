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
package org.apache.iceberg.schema;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Visitor class that accumulates the set of changes needed to evolve an existing schema into the
 * union of the existing and a new schema. Changes are added to an {@link UpdateSchema} operation.
 */
public class UnionByNameVisitor extends SchemaWithPartnerVisitor<Integer, Boolean> {

  private final UpdateSchema api;
  private final Schema partnerSchema;
  private final boolean caseSensitive;

  private UnionByNameVisitor(UpdateSchema api, Schema partnerSchema, boolean caseSensitive) {
    this.api = api;
    this.partnerSchema = partnerSchema;
    this.caseSensitive = caseSensitive;
  }

  /**
   * Adds changes needed to produce a union of two schemas to an {@link UpdateSchema} operation.
   *
   * <p>Changes are accumulated to evolve the existingSchema into a union with newSchema.
   *
   * @param api an UpdateSchema for adding changes
   * @param existingSchema an existing schema
   * @param newSchema a new schema to compare with the existing
   */
  public static void visit(UpdateSchema api, Schema existingSchema, Schema newSchema) {
    visit(api, existingSchema, newSchema, true);
  }

  /**
   * Adds changes needed to produce a union of two schemas to an {@link UpdateSchema} operation.
   *
   * <p>Changes are accumulated to evolve the existingSchema into a union with newSchema.
   *
   * @param api an UpdateSchema for adding changes
   * @param existingSchema an existing schema
   * @param caseSensitive when false, the case of schema's fields are ignored
   * @param newSchema a new schema to compare with the existing
   */
  public static void visit(
      UpdateSchema api, Schema existingSchema, Schema newSchema, boolean caseSensitive) {
    visit(
        newSchema,
        -1,
        new UnionByNameVisitor(api, existingSchema, caseSensitive),
        new PartnerIdByNameAccessors(existingSchema, caseSensitive));
  }

  @Override
  public Boolean struct(
      Types.StructType struct, Integer partnerId, List<Boolean> missingPositions) {
    if (partnerId == null) {
      return true;
    }

    List<Types.NestedField> fields = struct.fields();
    Types.StructType partnerStruct = findFieldType(partnerId).asStructType();
    IntStream.range(0, missingPositions.size())
        .forEach(
            pos -> {
              Boolean isMissing = missingPositions.get(pos);
              Types.NestedField field = fields.get(pos);
              if (isMissing) {
                addColumn(partnerId, field);
              } else {
                Types.NestedField nestedField =
                    caseSensitive
                        ? partnerStruct.field(field.name())
                        : partnerStruct.caseInsensitiveField(field.name());
                updateColumn(field, nestedField);
              }
            });

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
    updateColumn(list.fields().get(0), partnerList.fields().get(0));

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
    updateColumn(map.fields().get(0), partnerMap.fields().get(0));
    updateColumn(map.fields().get(1), partnerMap.fields().get(1));

    return false;
  }

  @Override
  public Boolean primitive(Type.PrimitiveType primitive, Integer partnerId) {
    return partnerId == null;
  }

  private Type findFieldType(int fieldId) {
    if (fieldId == -1) {
      return partnerSchema.asStruct();
    } else {
      return partnerSchema.findField(fieldId).type();
    }
  }

  private void addColumn(int parentId, Types.NestedField field) {
    String parentName = partnerSchema.findColumnName(parentId);
    api.addColumn(parentName, field.name(), field.type(), field.doc());
  }

  private void updateColumn(Types.NestedField field, Types.NestedField existingField) {
    String fullName = partnerSchema.findColumnName(existingField.fieldId());

    boolean needsOptionalUpdate = field.isOptional() && existingField.isRequired();
    boolean needsTypeUpdate =
        field.type().isPrimitiveType() && !field.type().equals(existingField.type());
    boolean needsDocUpdate = field.doc() != null && !field.doc().equals(existingField.doc());

    if (needsOptionalUpdate) {
      api.makeColumnOptional(fullName);
    }

    if (needsTypeUpdate) {
      api.updateColumn(fullName, field.type().asPrimitiveType());
    }

    if (needsDocUpdate) {
      api.updateColumnDoc(fullName, field.doc());
    }
  }

  private static class PartnerIdByNameAccessors implements PartnerAccessors<Integer> {
    private final Schema partnerSchema;
    private boolean caseSensitive = true;

    private PartnerIdByNameAccessors(Schema partnerSchema) {
      this.partnerSchema = partnerSchema;
    }

    private PartnerIdByNameAccessors(Schema partnerSchema, boolean caseSensitive) {
      this(partnerSchema);
      this.caseSensitive = caseSensitive;
    }

    @Override
    public Integer fieldPartner(Integer partnerFieldId, int fieldId, String name) {
      Types.StructType struct;
      if (partnerFieldId == -1) {
        struct = partnerSchema.asStruct();
      } else {
        struct = partnerSchema.findField(partnerFieldId).type().asStructType();
      }

      Types.NestedField field =
          caseSensitive ? struct.field(name) : struct.caseInsensitiveField(name);
      if (field != null) {
        return field.fieldId();
      }

      return null;
    }

    @Override
    public Integer mapKeyPartner(Integer partnerMapId) {
      Types.NestedField mapField = partnerSchema.findField(partnerMapId);
      if (mapField != null) {
        return mapField.type().asMapType().fields().get(0).fieldId();
      }

      return null;
    }

    @Override
    public Integer mapValuePartner(Integer partnerMapId) {
      Types.NestedField mapField = partnerSchema.findField(partnerMapId);
      if (mapField != null) {
        return mapField.type().asMapType().fields().get(1).fieldId();
      }

      return null;
    }

    @Override
    public Integer listElementPartner(Integer partnerListId) {
      Types.NestedField listField = partnerSchema.findField(partnerListId);
      if (listField != null) {
        return listField.type().asListType().fields().get(0).fieldId();
      }

      return null;
    }
  }
}
