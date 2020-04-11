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

package org.apache.iceberg.parquet;

import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.Preconditions;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

public class ParquetSchemaUtil {

  private ParquetSchemaUtil() {
  }

  public static MessageType convert(Schema schema, String name) {
    return new TypeToMessageType().convert(schema, name);
  }

  public static Schema convert(MessageType parquetSchema) {
    MessageTypeToType converter = new MessageTypeToType(parquetSchema);
    return new Schema(
        ParquetTypeVisitor.visit(parquetSchema, converter).asNestedType().fields(),
        converter.getAliases());
  }

  public static MessageType pruneColumns(MessageType fileSchema, Schema expectedSchema) {
    // column order must match the incoming type, so it doesn't matter that the ids are unordered
    Set<Integer> selectedIds = TypeUtil.getProjectedIds(expectedSchema);
    return (MessageType) ParquetTypeVisitor.visit(fileSchema, new PruneColumns(selectedIds));
  }

  /**
   * Prunes columns from a Parquet file schema that was written without field ids.
   * <p>
   * Files that were written without field ids are read assuming that schema evolution preserved
   * column order. Deleting columns was not allowed.
   * <p>
   * The order of columns in the resulting Parquet schema matches the Parquet file.
   *
   * @param fileSchema schema from a Parquet file that does not have field ids.
   * @param expectedSchema expected schema
   * @return a parquet schema pruned using the expected schema
   */
  public static MessageType pruneColumnsFallback(MessageType fileSchema, Schema expectedSchema) {
    Set<Integer> selectedIds = Sets.newHashSet();

    for (Types.NestedField field : expectedSchema.columns()) {
      selectedIds.add(field.fieldId());
    }

    MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();

    int ordinal = 1;
    for (Type type : fileSchema.getFields()) {
      if (selectedIds.contains(ordinal)) {
        builder.addField(type.withId(ordinal));
      }
      ordinal += 1;
    }

    return builder.named(fileSchema.getName());
  }

  public static boolean hasIds(MessageType fileSchema) {
    return ParquetTypeVisitor.visit(fileSchema, new HasIds(), true);
  }

  public static MessageType addFallbackIds(MessageType fileSchema, NameMapping nameMapping) {
    if (nameMapping == null) {
      MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();

      int ordinal = 1; // ids are assigned starting at 1
      for (Type type : fileSchema.getFields()) {
        builder.addField(type.withId(ordinal));
        ordinal += 1;
      }

      return builder.named(fileSchema.getName());
    } else {
      return (MessageType) ParquetTypeVisitor.visit(fileSchema, new AssignIdsByNameMapping(nameMapping), true);
    }
  }

  public static class HasIds extends ParquetTypeVisitor<Boolean> {
    @Override
    public Boolean message(MessageType message, List<Boolean> fields) {
      return struct(message, fields);
    }

    @Override
    public Boolean struct(GroupType struct, List<Boolean> hasIds) {
      for (Boolean hasId : hasIds) {
        if (hasId) {
          return true;
        }
      }
      return struct.getId() != null;
    }

    @Override
    public Boolean list(GroupType array, Boolean hasId) {
      if (hasId) {
        return true;
      } else {
        return array.getId() != null;
      }
    }

    @Override
    public Boolean map(GroupType map, Boolean keyHasId, Boolean valueHasId) {
      if (keyHasId || valueHasId) {
        return true;
      } else {
        return map.getId() != null;
      }
    }

    @Override
    public Boolean primitive(PrimitiveType primitive) {
      return primitive.getId() != null;
    }
  }

  public static class AssignIdsByNameMapping extends ParquetTypeVisitor<Type> {
    private final NameMapping nameMapping;

    public AssignIdsByNameMapping(NameMapping nameMapping) {
      this.nameMapping = nameMapping;
    }

    private String[] currentPath() {
      String[] path = new String[fieldNames.size()];
      if (!fieldNames.isEmpty()) {
        Iterator<String> iter = fieldNames.descendingIterator();
        for (int i = 0; iter.hasNext(); i += 1) {
          path[i] = iter.next();
        }
      }

      return path;
    }

    @Override
    public Type message(MessageType message, List<Type> fields) {
      MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();
      fields.stream().filter(Objects::nonNull).forEach(builder::addField);

      return builder.named(message.getName());
    }

    @Override
    public Type struct(GroupType struct, List<Type> types) {
      MappedField field = nameMapping.find(currentPath());
      if (field == null) {
        return null;
      }
      List<Type> actualTypes = types.stream().filter(Objects::nonNull).collect(Collectors.toList());

      return struct.withNewFields(actualTypes).withId(field.id());
    }

    @Override
    public Type list(GroupType list, Type elementType) {
      MappedField field = nameMapping.find(currentPath());
      if (field == null) {
        return null;
      }

      Preconditions.checkArgument(elementType != null,
          "List type must have element field");

      return org.apache.parquet.schema.Types.list(list.getRepetition())
          .element(elementType)
          .id(field.id())
          .named(list.getName());
    }

    @Override
    public Type map(GroupType map, Type keyType, Type valueType) {
      MappedField field = nameMapping.find(currentPath());
      if (field == null) {
        return null;
      }

      Preconditions.checkArgument(keyType != null && valueType != null,
          "Map type must have both key field and value field");

      return org.apache.parquet.schema.Types.map(map.getRepetition())
          .key(keyType)
          .value(valueType)
          .id(field.id())
          .named(map.getName());
    }

    @Override
    public Type primitive(PrimitiveType primitive) {
      MappedField field = nameMapping.find(currentPath());
      if (field == null) {
        return null;
      } else {
        return primitive.withId(field.id());
      }
    }
  }
}
