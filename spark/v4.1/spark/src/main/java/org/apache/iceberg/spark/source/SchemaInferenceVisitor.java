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
package org.apache.iceberg.spark.source;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.util.List;
import org.apache.iceberg.spark.data.ParquetWithSparkSchemaVisitor;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A visitor that infers variant shredding schemas by analyzing buffered rows of data. */
class SchemaInferenceVisitor extends ParquetWithSparkSchemaVisitor<Type> {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaInferenceVisitor.class);

  private final List<InternalRow> bufferedRows;
  private final StructType sparkSchema;
  private final SparkVariantShreddingAnalyzer analyzer;

  public SchemaInferenceVisitor(List<InternalRow> bufferedRows, StructType sparkSchema) {
    this.bufferedRows = bufferedRows;
    this.sparkSchema = sparkSchema;
    this.analyzer = new SparkVariantShreddingAnalyzer();
  }

  @Override
  public Type message(StructType sStruct, MessageType message, List<Type> fields) {
    MessageTypeBuilder builder = Types.buildMessage();

    for (Type field : fields) {
      if (field != null) {
        builder.addField(field);
      }
    }

    return builder.named(message.getName());
  }

  @Override
  public Type struct(StructType sStruct, GroupType struct, List<Type> fields) {
    Types.GroupBuilder<GroupType> builder = Types.buildGroup(struct.getRepetition());

    if (struct.getId() != null) {
      builder = builder.id(struct.getId().intValue());
    }

    for (Type field : fields) {
      if (field != null) {
        builder = builder.addField(field);
      }
    }

    return builder.named(struct.getName());
  }

  @Override
  public Type primitive(DataType sPrimitive, PrimitiveType primitive) {
    return primitive;
  }

  @Override
  public Type list(ArrayType sArray, GroupType array, Type element) {
    if (element == null) {
      return array;
    }

    GroupType repeatedGroup = array.getType(0).asGroupType();
    Types.GroupBuilder<GroupType> repeatedBuilder =
        Types.buildGroup(repeatedGroup.getRepetition()).addField(element);

    Types.GroupBuilder<GroupType> builder =
        Types.buildGroup(array.getRepetition()).as(LogicalTypeAnnotation.listType());
    if (array.getId() != null) {
      builder = builder.id(array.getId().intValue());
    }
    builder = builder.addField(repeatedBuilder.named(repeatedGroup.getName()));

    return builder.named(array.getName());
  }

  @Override
  public Type map(MapType sMap, GroupType map, Type key, Type value) {
    if (key == null && value == null) {
      return map;
    }

    GroupType repeatedGroup = map.getType(0).asGroupType();
    Types.GroupBuilder<GroupType> repeatedBuilder = Types.buildGroup(repeatedGroup.getRepetition());
    if (key != null) {
      repeatedBuilder = repeatedBuilder.addField(key);
    }
    if (value != null) {
      repeatedBuilder = repeatedBuilder.addField(value);
    }

    Types.GroupBuilder<GroupType> builder =
        Types.buildGroup(map.getRepetition()).as(LogicalTypeAnnotation.mapType());
    if (map.getId() != null) {
      builder = builder.id(map.getId().intValue());
    }
    builder = builder.addField(repeatedBuilder.named(repeatedGroup.getName()));

    return builder.named(map.getName());
  }

  @Override
  public Type variant(VariantType sVariant, GroupType variant) {
    int variantFieldIndex = getFieldIndex(currentPath());

    if (!bufferedRows.isEmpty() && variantFieldIndex >= 0) {
      Type shreddedType = analyzer.analyzeAndCreateSchema(bufferedRows, variantFieldIndex);
      if (shreddedType != null) {
        Types.GroupBuilder<GroupType> builder =
            Types.buildGroup(variant.getRepetition())
                .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION));
        if (variant.getId() != null) {
          builder = builder.id(variant.getId().intValue());
        }
        return builder
            .required(BINARY)
            .named("metadata")
            .optional(BINARY)
            .named("value")
            .addField(shreddedType)
            .named(variant.getName());
      }
    }

    return variant;
  }

  private int getFieldIndex(String[] path) {
    if (path == null || path.length == 0) {
      return -1;
    }

    if (path.length == 1) {
      // Top-level field - direct lookup
      String fieldName = path[0];
      for (int i = 0; i < sparkSchema.fields().length; i++) {
        if (sparkSchema.fields()[i].name().equals(fieldName)) {
          return i;
        }
      }
    } else {
      // TODO: Implement full nested field resolution
      LOG.warn("Nested variant shredding is not supported. Path: {}", String.join(".", path));
    }

    return -1;
  }
}
