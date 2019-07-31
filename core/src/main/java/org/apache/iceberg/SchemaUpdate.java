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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Schema evolution API implementation.
 */
class SchemaUpdate implements UpdateSchema {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUpdate.class);
  private static final int TABLE_ROOT_ID = -1;

  private final TableOperations ops;
  private final TableMetadata base;
  private final Schema schema;
  private final List<Integer> deletes = Lists.newArrayList();
  private final Map<Integer, Types.NestedField> updates = Maps.newHashMap();
  private final Multimap<Integer, Types.NestedField> adds =
      Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
  private int lastColumnId;

  SchemaUpdate(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.schema = base.schema();
    this.lastColumnId = base.lastColumnId();
  }

  /**
   * For testing only.
   */
  SchemaUpdate(Schema schema, int lastColumnId) {
    this.ops = null;
    this.base = null;
    this.schema = schema;
    this.lastColumnId = lastColumnId;
  }

  @Override
  public UpdateSchema addColumn(String name, Type type, String doc) {
    Preconditions.checkArgument(!name.contains("."),
        "Cannot add column with ambiguous name: %s, use addColumn(parent, name, type)", name);
    return addColumn(null, name, type, doc);
  }

  @Override
  public UpdateSchema addColumn(String parent, String name, Type type, String doc) {
    int parentId = TABLE_ROOT_ID;
    if (parent != null) {
      Types.NestedField parentField = schema.findField(parent);
      Preconditions.checkArgument(parentField != null, "Cannot find parent struct: %s", parent);
      Type parentType = parentField.type();
      if (parentType.isNestedType()) {
        Type.NestedType nested = parentType.asNestedType();
        if (nested.isMapType()) {
          // fields are added to the map value type
          parentField = nested.asMapType().fields().get(1);
        } else if (nested.isListType()) {
          // fields are added to the element type
          parentField = nested.asListType().fields().get(0);
        }
      }
      Preconditions.checkArgument(
          parentField.type().isNestedType() && parentField.type().asNestedType().isStructType(),
          "Cannot add to non-struct column: %s: %s", parent, parentField.type());
      parentId = parentField.fieldId();
      Preconditions.checkArgument(!deletes.contains(parentId),
          "Cannot add to a column that will be deleted: %s", parent);
      Preconditions.checkArgument(schema.findField(parent + "." + name) == null,
          "Cannot add column, name already exists: %s.%s", parent, name);
    } else {
      Preconditions.checkArgument(schema.findField(name) == null,
          "Cannot add column, name already exists: %s", name);
    }

    // assign new IDs in order
    int newId = assignNewColumnId();
    adds.put(parentId, optional(newId, name,
        TypeUtil.assignFreshIds(type, this::assignNewColumnId), doc));

    return this;
  }

  @Override
  public UpdateSchema deleteColumn(String name) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot delete missing column: %s", name);
    Preconditions.checkArgument(!adds.containsKey(field.fieldId()),
        "Cannot delete a column that has additions: %s", name);
    Preconditions.checkArgument(!updates.containsKey(field.fieldId()),
        "Cannot delete a column that has updates: %s", name);

    deletes.add(field.fieldId());

    return this;
  }

  @Override
  public UpdateSchema renameColumn(String name, String newName) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot rename missing column: %s", name);
    Preconditions.checkArgument(newName != null, "Cannot rename a column to null");
    Preconditions.checkArgument(!deletes.contains(field.fieldId()),
        "Cannot rename a column that will be deleted: %s", field.name());

    // merge with an update, if present
    int fieldId = field.fieldId();
    Types.NestedField update = updates.get(fieldId);
    if (update != null) {
      updates.put(fieldId, required(fieldId, newName, update.type(), update.doc()));
    } else {
      updates.put(fieldId, required(fieldId, newName, field.type(), field.doc()));
    }

    return this;
  }

  @Override
  public UpdateSchema updateColumn(String name, Type.PrimitiveType newType) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot update missing column: %s", name);
    Preconditions.checkArgument(!deletes.contains(field.fieldId()),
        "Cannot update a column that will be deleted: %s", field.name());
    Preconditions.checkArgument(TypeUtil.isPromotionAllowed(field.type(), newType),
        "Cannot change column type: %s: %s -> %s", name, field.type(), newType);

    // merge with a rename, if present
    int fieldId = field.fieldId();
    Types.NestedField rename = updates.get(fieldId);
    if (rename != null) {
      updates.put(fieldId, required(fieldId, rename.name(), newType, rename.doc()));
    } else {
      updates.put(fieldId, required(fieldId, field.name(), newType, field.doc()));
    }

    return this;
  }

  @Override
  public UpdateSchema updateColumnDoc(String name, String doc) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot update missing column: %s", name);
    Preconditions.checkArgument(!deletes.contains(field.fieldId()),
        "Cannot update a column that will be deleted: %s", field.name());

    // merge with a rename or update, if present
    int fieldId = field.fieldId();
    Types.NestedField update = updates.get(fieldId);
    if (update != null) {
      updates.put(fieldId, required(fieldId, update.name(), update.type(), doc));
    } else {
      updates.put(fieldId, required(fieldId, field.name(), field.type(), doc));
    }

    return this;
  }

  /**
   * Apply the pending changes to the original schema and returns the result.
   * <p>
   * This does not result in a permanent update.
   *
   * @return the result Schema when all pending updates are applied
   */
  @Override
  public Schema apply() {
    return applyChanges(schema, deletes, updates, adds);
  }

  @Override
  public void commit() {
    TableMetadata update = applyChangesToMapping(base.updateSchema(apply(), lastColumnId));
    ops.commit(base, update);
  }

  private int assignNewColumnId() {
    int next = lastColumnId + 1;
    this.lastColumnId = next;
    return next;
  }

  private TableMetadata applyChangesToMapping(TableMetadata metadata) {
    String mappingJson = metadata.property(TableProperties.DEFAULT_NAME_MAPPING, null);
    if (mappingJson != null) {
      try {
        // parse and update the mapping
        NameMapping mapping = NameMappingParser.fromJson(mappingJson);
        NameMapping updated = MappingUtil.update(mapping, updates, adds);

        // replace the table property
        Map<String, String> updatedProperties = Maps.newHashMap();
        updatedProperties.putAll(metadata.properties());
        updatedProperties.put(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(updated));

        return metadata.replaceProperties(updatedProperties);

      } catch (RuntimeException e) {
        // log the error, but do not fail the update
        LOG.warn("Failed to update external schema mapping: {}", mappingJson, e);
      }
    }

    return metadata;
  }

  private static Schema applyChanges(Schema schema, List<Integer> deletes,
                                     Map<Integer, Types.NestedField> updates,
                                     Multimap<Integer, Types.NestedField> adds) {
    Types.StructType struct = TypeUtil
        .visit(schema, new ApplyChanges(deletes, updates, adds))
        .asNestedType().asStructType();
    return new Schema(struct.fields());
  }

  private static class ApplyChanges extends TypeUtil.SchemaVisitor<Type> {
    private final List<Integer> deletes;
    private final Map<Integer, Types.NestedField> updates;
    private final Multimap<Integer, Types.NestedField> adds;

    private ApplyChanges(List<Integer> deletes,
                        Map<Integer, Types.NestedField> updates,
                        Multimap<Integer, Types.NestedField> adds) {
      this.deletes = deletes;
      this.updates = updates;
      this.adds = adds;
    }

    @Override
    public Type schema(Schema schema, Type structResult) {
      Collection<Types.NestedField> newColumns = adds.get(TABLE_ROOT_ID);
      if (newColumns != null) {
        return addFields(structResult.asNestedType().asStructType(), newColumns);
      }

      return structResult;
    }

    @Override
    public Type struct(Types.StructType struct, List<Type> fieldResults) {
      boolean hasChange = false;
      List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fieldResults.size());
      for (int i = 0; i < fieldResults.size(); i += 1) {
        Type resultType = fieldResults.get(i);
        if (resultType == null) {
          hasChange = true;
          continue;
        }

        Types.NestedField field = struct.fields().get(i);
        String name = field.name();
        String doc = field.doc();
        Types.NestedField update = updates.get(field.fieldId());
        if (update != null) {
          name = update.name();
          doc = update.doc();
        }

        if (!name.equals(field.name()) ||
            field.type() != resultType ||
            !Objects.equals(doc, field.doc())) {
          hasChange = true;
          if (field.isOptional()) {
            newFields.add(optional(field.fieldId(), name, resultType, doc));
          } else {
            newFields.add(required(field.fieldId(), name, resultType, doc));
          }
        } else {
          newFields.add(field);
        }
      }

      if (hasChange) {
        // TODO: What happens if there are no fields left?
        return Types.StructType.of(newFields);
      }

      return struct;
    }

    @Override
    public Type field(Types.NestedField field, Type fieldResult) {
      // the API validates deletes, updates, and additions don't conflict
      // handle deletes
      int fieldId = field.fieldId();
      if (deletes.contains(fieldId)) {
        return null;
      }

      // handle updates
      Types.NestedField update = updates.get(field.fieldId());
      if (update != null && update.type() != field.type()) {
        // rename is handled in struct, but struct needs the correct type from the field result
        return update.type();
      }

      // handle adds
      Collection<Types.NestedField> newFields = adds.get(fieldId);
      if (newFields != null && !newFields.isEmpty()) {
        return addFields(fieldResult.asNestedType().asStructType(), newFields);
      }

      return fieldResult;
    }

    @Override
    public Type list(Types.ListType list, Type result) {
      // use field to apply updates
      Type elementResult = field(list.fields().get(0), result);
      if (elementResult == null) {
        throw new IllegalArgumentException("Cannot delete element type from list: " + list);
      }

      if (list.elementType() == elementResult) {
        return list;
      }

      if (list.isElementOptional()) {
        return Types.ListType.ofOptional(list.elementId(), elementResult);
      } else {
        return Types.ListType.ofRequired(list.elementId(), elementResult);
      }
    }

    @Override
    public Type map(Types.MapType map, Type kResult, Type vResult) {
      // if any updates are intended for the key, throw an exception
      int keyId = map.fields().get(0).fieldId();
      if (deletes.contains(keyId)) {
        throw new IllegalArgumentException("Cannot delete map keys: " + map);
      } else if (updates.containsKey(keyId)) {
        throw new IllegalArgumentException("Cannot update map keys: " + map);
      } else if (adds.containsKey(keyId)) {
        throw new IllegalArgumentException("Cannot add fields to map keys: " + map);
      } else if (!map.keyType().equals(kResult)) {
        throw new IllegalArgumentException("Cannot alter map keys: " + map);
      }

      // use field to apply updates to the value
      Type valueResult = field(map.fields().get(1), vResult);
      if (valueResult == null) {
        throw new IllegalArgumentException("Cannot delete value type from map: " + map);
      }

      if (map.valueType() == valueResult) {
        return map;
      }

      if (map.isValueOptional()) {
        return Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), valueResult);
      } else {
        return Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), valueResult);
      }
    }

    @Override
    public Type primitive(Type.PrimitiveType primitive) {
      return primitive;
    }
  }

  private static Types.StructType addFields(Types.StructType struct,
                                            Collection<Types.NestedField> adds) {
    List<Types.NestedField> newFields = Lists.newArrayList(struct.fields());
    newFields.addAll(adds);
    return Types.StructType.of(newFields);
  }
}
