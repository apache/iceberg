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
package org.apache.iceberg.mapping;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class MappingUtil {
  private static final Joiner DOT = Joiner.on('.');

  private MappingUtil() {}

  /**
   * Create a name-based mapping for a schema.
   *
   * <p>The mapping returned by this method will use the schema's name for each field.
   *
   * @param schema a {@link Schema}
   * @return a {@link NameMapping} initialized with the schema's fields and names
   */
  public static NameMapping create(Schema schema) {
    return new NameMapping(TypeUtil.visit(schema, CreateMapping.INSTANCE));
  }

  /**
   * Update a name-based mapping using changes to a schema.
   *
   * @param mapping a name-based mapping
   * @param updates a map from field ID to updated field definitions
   * @param adds a map from parent field ID to nested fields to be added
   * @return an updated mapping with names added to renamed fields and the mapping extended for new
   *     fields
   */
  public static NameMapping update(
      NameMapping mapping,
      Map<Integer, Types.NestedField> updates,
      Multimap<Integer, Types.NestedField> adds) {
    return new NameMapping(visit(mapping, new UpdateMapping(updates, adds)));
  }

  static Map<Integer, MappedField> indexById(MappedFields mapping) {
    return visit(mapping, new IndexById());
  }

  static Map<String, MappedField> indexByName(MappedFields mapping) {
    return visit(mapping, IndexByName.INSTANCE);
  }

  private static class UpdateMapping implements Visitor<MappedFields, MappedField> {
    private final Map<Integer, Types.NestedField> updates;
    private final Multimap<Integer, Types.NestedField> adds;

    private UpdateMapping(
        Map<Integer, Types.NestedField> updates, Multimap<Integer, Types.NestedField> adds) {
      this.updates = updates;
      this.adds = adds;
    }

    @Override
    public MappedFields mapping(NameMapping mapping, MappedFields result) {
      return addNewFields(result, -1 /* parent ID used to add top-level fields */);
    }

    @Override
    public MappedFields fields(MappedFields fields, List<MappedField> fieldResults) {
      ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
      fieldResults.stream()
          .map(MappedField::id)
          .filter(Objects::nonNull)
          .map(updates::get)
          .filter(Objects::nonNull)
          .forEach(field -> builder.put(field.name(), field.fieldId()));
      Map<String, Integer> updateAssignments = builder.build();

      return MappedFields.of(
          Lists.transform(fieldResults, field -> removeReassignedNames(field, updateAssignments)));
    }

    @Override
    public MappedField field(MappedField field, MappedFields fieldResult) {
      // update this field's names
      Set<String> fieldNames = Sets.newHashSet(field.names());
      Types.NestedField update = updates.get(field.id());
      if (update != null) {
        fieldNames.add(update.name());
      }

      // add a new mapping for any new nested fields
      MappedFields nestedMapping = addNewFields(fieldResult, field.id());
      return MappedField.of(field.id(), fieldNames, nestedMapping);
    }

    private MappedFields addNewFields(MappedFields mapping, int parentId) {
      Collection<Types.NestedField> fieldsToAdd = adds.get(parentId);
      if (fieldsToAdd == null || fieldsToAdd.isEmpty()) {
        return mapping;
      }

      List<MappedField> newFields = Lists.newArrayList();
      for (Types.NestedField add : fieldsToAdd) {
        MappedFields nestedMapping = TypeUtil.visit(add.type(), CreateMapping.INSTANCE);
        newFields.add(MappedField.of(add.fieldId(), add.name(), nestedMapping));
      }

      if (mapping == null || mapping.fields().isEmpty()) {
        return MappedFields.of(newFields);
      }

      ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
      fieldsToAdd.forEach(field -> builder.put(field.name(), field.fieldId()));
      Map<String, Integer> assignments = builder.build();

      // create a copy of fields that can be updated (append new fields, replace existing for
      // reassignment)
      List<MappedField> fields = Lists.newArrayList();
      for (MappedField field : mapping.fields()) {
        fields.add(removeReassignedNames(field, assignments));
      }

      fields.addAll(newFields);

      return MappedFields.of(fields);
    }

    private static MappedField removeReassignedNames(
        MappedField field, Map<String, Integer> assignments) {
      MappedField newField = field;
      for (String name : field.names()) {
        Integer assignedId = assignments.get(name);
        if (assignedId != null && !Objects.equals(assignedId, field.id())) {
          newField = removeName(field, name);
        }
      }
      return newField;
    }

    private static MappedField removeName(MappedField field, String name) {
      return MappedField.of(
          field.id(), Sets.difference(field.names(), Sets.newHashSet(name)), field.nestedMapping());
    }
  }

  private static class IndexByName
      implements Visitor<Map<String, MappedField>, Map<String, MappedField>> {
    static final IndexByName INSTANCE = new IndexByName();

    @Override
    public Map<String, MappedField> mapping(NameMapping mapping, Map<String, MappedField> result) {
      return result;
    }

    @Override
    public Map<String, MappedField> fields(
        MappedFields fields, List<Map<String, MappedField>> fieldResults) {
      // merge the results of each field
      ImmutableMap.Builder<String, MappedField> builder = ImmutableMap.builder();
      for (Map<String, MappedField> results : fieldResults) {
        builder.putAll(results);
      }
      return builder.build();
    }

    @Override
    public Map<String, MappedField> field(MappedField field, Map<String, MappedField> fieldResult) {
      ImmutableMap.Builder<String, MappedField> builder = ImmutableMap.builder();

      if (fieldResult != null) {
        for (String name : field.names()) {
          for (Map.Entry<String, MappedField> entry : fieldResult.entrySet()) {
            String fullName = DOT.join(name, entry.getKey());
            builder.put(fullName, entry.getValue());
          }
        }
      }

      for (String name : field.names()) {
        builder.put(name, field);
      }

      return builder.build();
    }
  }

  private static class IndexById
      implements Visitor<Map<Integer, MappedField>, Map<Integer, MappedField>> {
    private final Map<Integer, MappedField> result = Maps.newHashMap();

    @Override
    public Map<Integer, MappedField> mapping(
        NameMapping mapping, Map<Integer, MappedField> fieldsResult) {
      return fieldsResult;
    }

    @Override
    public Map<Integer, MappedField> fields(
        MappedFields fields, List<Map<Integer, MappedField>> fieldResults) {
      return result;
    }

    @Override
    public Map<Integer, MappedField> field(
        MappedField field, Map<Integer, MappedField> fieldResult) {
      Preconditions.checkState(
          !result.containsKey(field.id()), "Invalid mapping: ID %s is not unique", field.id());
      result.put(field.id(), field);
      return result;
    }
  }

  private interface Visitor<S, T> {
    S mapping(NameMapping mapping, S result);

    S fields(MappedFields fields, List<T> fieldResults);

    T field(MappedField field, S fieldResult);
  }

  private static <S, T> S visit(NameMapping mapping, Visitor<S, T> visitor) {
    return visitor.mapping(mapping, visit(mapping.asMappedFields(), visitor));
  }

  private static <S, T> S visit(MappedFields mapping, Visitor<S, T> visitor) {
    if (mapping == null) {
      return null;
    }

    List<T> fieldResults = Lists.newArrayList();
    for (MappedField field : mapping.fields()) {
      fieldResults.add(visitor.field(field, visit(field.nestedMapping(), visitor)));
    }

    return visitor.fields(mapping, fieldResults);
  }

  private static class CreateMapping extends TypeUtil.SchemaVisitor<MappedFields> {
    private static final CreateMapping INSTANCE = new CreateMapping();

    private CreateMapping() {}

    @Override
    public MappedFields schema(Schema schema, MappedFields structResult) {
      return structResult;
    }

    @Override
    public MappedFields struct(Types.StructType struct, List<MappedFields> fieldResults) {
      List<MappedField> fields = Lists.newArrayListWithExpectedSize(fieldResults.size());

      for (int i = 0; i < fieldResults.size(); i += 1) {
        Types.NestedField field = struct.fields().get(i);
        MappedFields result = fieldResults.get(i);
        fields.add(MappedField.of(field.fieldId(), field.name(), result));
      }

      return MappedFields.of(fields);
    }

    @Override
    public MappedFields field(Types.NestedField field, MappedFields fieldResult) {
      return fieldResult;
    }

    @Override
    public MappedFields list(Types.ListType list, MappedFields elementResult) {
      return MappedFields.of(MappedField.of(list.elementId(), "element", elementResult));
    }

    @Override
    public MappedFields map(Types.MapType map, MappedFields keyResult, MappedFields valueResult) {
      return MappedFields.of(
          MappedField.of(map.keyId(), "key", keyResult),
          MappedField.of(map.valueId(), "value", valueResult));
    }

    @Override
    public MappedFields primitive(Type.PrimitiveType primitive) {
      return null; // no mapping because primitives have no nested fields
    }
  }
}
