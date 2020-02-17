package org.apache.iceberg;

import static org.apache.iceberg.SchemaUpdate.findParentField;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.iceberg.mapping.*;
import org.apache.iceberg.types.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

  @Override
  public UpdateNameMapping addAliases(String name, Iterable<String> aliases) {
    Preconditions.checkArgument(schema.findField(name) != null, "Cannot find column: %s", name);
    int fieldId = schema.findField(name).fieldId();
    addAliases(fieldId, aliases);
    return this;
  }

  @Override
  public UpdateNameMapping addAliases(String parent, String name, Iterable<String> aliases) {
    Types.NestedField parentField = findParentField(parent, schema);
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

  private void addAliases(int fieldId, Iterable<String> aliases) {
    adds.compute(fieldId, (dontCare, oldValue) -> {
      if (oldValue == null) {
        return aliases;
      } else {
        return ImmutableList.<String>builder().addAll(aliases).build();
      }
    });
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

    private static Iterable<String> merge(Iterable<String> a, Iterable<String> b) {
      if (a == null) {
        return b;
      }

      if (b == null) {
        return a;
      }

      return ImmutableList.<String>builder()
          .addAll(a)
          .addAll(b)
          .build();
    }
  }
}
