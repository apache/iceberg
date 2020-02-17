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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * NameMapping evolution API implementation.
 */
class NameMappingUpdate implements UpdateNameMapping {
  private final Schema schema;
  private final NameMapping nameMapping;
  private final UpdateProperties updateProperties;
  private final Map<Integer, Iterable<String>> adds = new ConcurrentHashMap<>();

  NameMappingUpdate(TableOperations ops) {
    TableMetadata tableMetadata = ops.current();
    this.schema = tableMetadata.schema();
    this.nameMapping = NameMappingParser.fromJson(tableMetadata.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    this.updateProperties = new PropertiesUpdate(ops);
  }

  /**
   * For testing only.
   */
  NameMappingUpdate(
      Schema schema, NameMapping nameMapping, UpdateProperties updateProperties) {
    this.schema = schema;
    this.nameMapping = nameMapping;
    this.updateProperties = updateProperties;
  }

  private void addAliases(int fieldId, Iterable<String> aliases) {
    adds.compute(fieldId, (dontCare, oldValue) -> {
      if (oldValue == null) {
        return aliases;
      } else {
        return ImmutableList.<String>builder().addAll(aliases).build();
      }
    });
  }

  @Override
  public UpdateNameMapping addAliases(String name, Iterable<String> aliases) {
    Preconditions.checkArgument(schema.findField(name) != null, "Cannot find column: %s", name);
    int fieldId = schema.findField(name).fieldId();
    addAliases(fieldId, aliases);
    return this;
  }

  @Override
  public UpdateNameMapping addAliases(String parent, String name, Iterable<String> aliases) {
    Types.NestedField parentField = SchemaUpdate.findParentField(parent, schema);
    Preconditions.checkArgument(schema.findField(parent + "." + name) != null,
        "Cannot add alias, name doesn't exists: %s.%s", parent, name);
    addAliases(schema.findField(parent + "." + name).fieldId(), aliases);
    return this;
  }

  @Override
  public NameMapping apply() {
    MappedFields fields = TypeUtil.visit(schema, new UpdateNameMappingVisitor(nameMapping, adds));
    return NameMapping.of(fields);
  }

  @Override
  public void commit() {
    NameMapping goalState = apply();
    updateProperties
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(goalState))
        .commit();
  }

  private static class UpdateNameMappingVisitor extends TypeUtil.SchemaVisitor<MappedFields> {
    private final NameMapping nameMapping;
    private final Map<Integer, Iterable<String>> aliasAdditions;

    private UpdateNameMappingVisitor(
        NameMapping nameMapping, Map<Integer, Iterable<String>> aliasAdditions) {
      this.nameMapping = nameMapping;
      this.aliasAdditions = aliasAdditions;
    }

    @Override
    public MappedFields schema(Schema schema, MappedFields structResult) {
      return structResult;
    }

    @Override
    public MappedFields struct(
        Types.StructType struct, List<MappedFields> fieldResults) {
      List<MappedField> fields = Lists.newArrayListWithExpectedSize(fieldResults.size());

      for (int i = 0; i < fieldResults.size(); i += 1) {
        Types.NestedField field = struct.fields().get(i);
        MappedFields result = fieldResults.get(i);
        fields.add(result.field(field.fieldId()));
      }

      return MappedFields.of(fields);
    }

    @Override
    public MappedFields field(Types.NestedField field, MappedFields fieldResult) {
      MappedField mappedField = nameMapping.find(field.fieldId());

      return MappedFields.of(
          MappedField.of(
              mappedField.id(),
              merge(mappedField.names(), aliasAdditions.get(field.fieldId())),
              fieldResult != null ? fieldResult : mappedField.nestedMapping()));
    }

    @Override
    public MappedFields list(Types.ListType list, MappedFields elementResult) {
      return MappedFields.of(
          MappedField.of(
              list.elementId(),
              nameMapping.find(list.elementId()).names(),
              elementResult));
    }

    @Override
    public MappedFields map(Types.MapType map, MappedFields keyResult, MappedFields valueResult) {
      MappedField keyMappedField = nameMapping.find(map.keyId());
      MappedField valueMappedField = nameMapping.find(map.valueId());
      return MappedFields.of(
          MappedField.of(map.keyId(), keyMappedField.names(), keyResult),
          MappedField.of(map.valueId(), valueMappedField.names(), valueResult)
      );
    }

    @Override
    public MappedFields primitive(Type.PrimitiveType primitive) {
      return null; // no mapping because primitives have no nested fields
    }

    private static Iterable<String> merge(Iterable<String> first, Iterable<String> second) {
      if (first == null) {
        return second;
      }

      if (second == null) {
        return first;
      }

      return ImmutableList.<String>builder()
          .addAll(first)
          .addAll(second)
          .build();
    }
  }
}
