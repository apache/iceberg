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
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

public class ParquetSchemaUtil {
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
    try {
      // Try to convert the type to Iceberg. If an ID assignment is needed, return false.
      ParquetTypeVisitor.visit(fileSchema, new MessageTypeToType(fileSchema) {
        @Override
        protected int nextId() {
          throw new IllegalStateException("Needed to assign ID");
        }
      });

      // no assignment was needed
      return true;

    } catch (IllegalStateException e) {
      // at least one field was missing an id.
      return false;
    }
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
}
