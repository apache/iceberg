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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

public class ParquetSchemaUtil {

  private ParquetSchemaUtil() {}

  public static MessageType convert(Schema schema, String name) {
    return new TypeToMessageType().convert(schema, name);
  }

  /**
   * Converts a Parquet schema to an Iceberg schema. Fields without IDs are kept and assigned
   * fallback IDs.
   *
   * @param parquetSchema a Parquet schema
   * @return a matching Iceberg schema for the provided Parquet schema
   */
  public static Schema convert(MessageType parquetSchema) {
    // if the Parquet schema does not contain ids, we assign fallback ids to top-level fields
    // all remaining fields will get ids >= 1000 to avoid pruning columns without ids
    MessageType parquetSchemaWithIds =
        hasIds(parquetSchema) ? parquetSchema : addFallbackIds(parquetSchema);
    AtomicInteger nextId = new AtomicInteger(1000);
    return convertInternal(parquetSchemaWithIds, name -> nextId.getAndIncrement());
  }

  /**
   * Converts a Parquet schema to an Iceberg schema and prunes fields without IDs.
   *
   * @param parquetSchema a Parquet schema
   * @return a matching Iceberg schema for the provided Parquet schema
   */
  public static Schema convertAndPrune(MessageType parquetSchema) {
    return convertInternal(parquetSchema, name -> null);
  }

  private static Schema convertInternal(
      MessageType parquetSchema, Function<String[], Integer> nameToIdFunc) {
    MessageTypeToType converter = new MessageTypeToType(nameToIdFunc);
    return new Schema(
        ParquetTypeVisitor.visit(parquetSchema, converter).asNestedType().fields(),
        converter.getAliases());
  }

  public static MessageType pruneColumns(MessageType fileSchema, Schema expectedSchema) {
    // column order must match the incoming type, so it doesn't matter that the ids are unordered
    Set<Integer> selectedIds = TypeUtil.getProjectedIds(expectedSchema);

    // Add Value and Metadata subfield ids for Variant type. These subfields are parquet specific and unknown to Iceberg,
    // getProjectedIds() can't add those ids.
    Set<Integer> variantSelectedIds = new HashSet<>();
    List<Type> fields = fileSchema.getFields();
    for (Types.NestedField iField : expectedSchema.columns()) {
      if (iField.type() == Types.VariantType.get()) {
        GroupType field = (GroupType)fields.get(fileSchema.getFieldIndex(iField.name()));
        variantSelectedIds.add(field.getFields().get(0).getId().intValue());
        variantSelectedIds.add(field.getFields().get(1).getId().intValue());
      }
    }

    selectedIds = ImmutableSet.<Integer>builder().addAll(selectedIds).addAll(variantSelectedIds).build();
    return (MessageType) ParquetTypeVisitor.visit(fileSchema, new PruneColumns(selectedIds));
  }

  /**
   * Prunes columns from a Parquet file schema that was written without field ids.
   *
   * <p>Files that were written without field ids are read assuming that schema evolution preserved
   * column order. Deleting columns was not allowed.
   *
   * <p>The order of columns in the resulting Parquet schema matches the Parquet file.
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
    return ParquetTypeVisitor.visit(fileSchema, new HasIds());
  }

  public static MessageType addFallbackIds(MessageType fileSchema) {
    MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();

    int ordinal = 1; // ids are assigned starting at 1
    for (Type type : fileSchema.getFields()) {
      builder.addField(type.withId(ordinal));
      ordinal += 1;
    }

    return builder.named(fileSchema.getName());
  }

  public static MessageType applyNameMapping(MessageType fileSchema, NameMapping nameMapping) {
    return (MessageType) ParquetTypeVisitor.visit(fileSchema, new ApplyNameMapping(nameMapping));
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
      return hasId || array.getId() != null;
    }

    @Override
    public Boolean map(GroupType map, Boolean keyHasId, Boolean valueHasId) {
      return keyHasId || valueHasId || map.getId() != null;
    }

    @Override
    public Boolean primitive(PrimitiveType primitive) {
      return primitive.getId() != null;
    }
  }

  public static Type determineListElementType(GroupType array) {
    Type repeated = array.getFields().get(0);
    boolean isOldListElementType = isOldListElementType(array);

    return isOldListElementType ? repeated : repeated.asGroupType().getType(0);
  }

  // Parquet LIST backwards-compatibility rules.
  // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  static boolean isOldListElementType(GroupType list) {
    Type repeatedType = list.getFields().get(0);
    String parentName = list.getName();

    return
    // For legacy 2-level list types with primitive element type, e.g.:
    //
    //    // ARRAY<INT> (nullable list, non-null elements)
    //    optional group my_list (LIST) {
    //      repeated int32 element;
    //    }
    //
    repeatedType.isPrimitive()
        ||
        // For legacy 2-level list types whose element type is a group type with 2 or more fields,
        // e.g.:
        //
        //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
        //    optional group my_list (LIST) {
        //      repeated group element {
        //        required binary str (UTF8);
        //        required int32 num;
        //      };
        //    }
        //
        repeatedType.asGroupType().getFieldCount() > 1
        ||
        // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0), e.g.:
        //
        //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
        //    optional group my_list (LIST) {
        //      repeated group array {
        //        required binary str (UTF8);
        //      };
        //    }
        repeatedType.getName().equals("array")
        ||
        // For Parquet data generated by parquet-thrift, e.g.:
        //
        //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
        //    optional group my_list (LIST) {
        //      repeated group my_list_tuple {
        //        required binary str (UTF8);
        //      };
        //    }
        //
        repeatedType.getName().equals(parentName + "_tuple");
  }
}
