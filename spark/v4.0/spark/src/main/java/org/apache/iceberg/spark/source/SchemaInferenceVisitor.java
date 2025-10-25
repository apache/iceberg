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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.iceberg.parquet.ParquetVariantUtil;
import org.apache.iceberg.spark.data.ParquetWithSparkSchemaVisitor;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
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
import org.apache.spark.unsafe.types.VariantVal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A visitor that infers variant shredding schemas by analyzing buffered rows of data. This visitor
 * can be plugged into ParquetWithSparkSchemaVisitor.visit() to create a shredded MessageType based
 * on actual variant data content.
 *
 * <p>The visitor uses the field names tracked during traversal to look up the correct field index
 * in the Spark schema, allowing it to access the corresponding value in the rows for schema
 * inference. It searches through all buffered rows to find the first non-null variant value for
 * schema inference.
 */
public class SchemaInferenceVisitor extends ParquetWithSparkSchemaVisitor<Type> {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaInferenceVisitor.class);

  private final List<InternalRow> bufferedRows;
  private final StructType sparkSchema;

  public SchemaInferenceVisitor(List<InternalRow> bufferedRows, StructType sparkSchema) {
    this.bufferedRows = bufferedRows;
    this.sparkSchema = sparkSchema;
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
    Types.GroupBuilder<GroupType> builder =
        Types.buildGroup(array.getRepetition()).as(LogicalTypeAnnotation.listType());

    if (array.getId() != null) {
      builder = builder.id(array.getId().intValue());
    }

    if (element != null) {
      builder = builder.addField(element);
    }

    return builder.named(array.getName());
  }

  @Override
  public Type map(MapType sMap, GroupType map, Type key, Type value) {
    Types.GroupBuilder<GroupType> builder =
        Types.buildGroup(map.getRepetition()).as(LogicalTypeAnnotation.mapType());

    if (map.getId() != null) {
      builder = builder.id(map.getId().intValue());
    }

    if (key != null) {
      builder = builder.addField(key);
    }
    if (value != null) {
      builder = builder.addField(value);
    }

    return builder.named(map.getName());
  }

  @Override
  public Type variant(VariantType sVariant, GroupType variant) {
    int variantFieldIndex = getFieldIndex(currentPath());

    // Find the first non-null variant value from buffered rows for schema inference
    // This ensures we can infer a schema even if the first rows has null variant values
    if (!bufferedRows.isEmpty() && variantFieldIndex >= 0) {
      for (InternalRow row : bufferedRows) {
        if (!row.isNullAt(variantFieldIndex)) {
          VariantVal variantVal = row.getVariant(variantFieldIndex);
          if (variantVal != null) {
            VariantValue variantValue =
                VariantValue.from(
                    VariantMetadata.from(
                        ByteBuffer.wrap(variantVal.getMetadata()).order(ByteOrder.LITTLE_ENDIAN)),
                    ByteBuffer.wrap(variantVal.getValue()).order(ByteOrder.LITTLE_ENDIAN));

            Type shreddedType = ParquetVariantUtil.toParquetSchema(variantValue);
            if (shreddedType != null) {
              return Types.buildGroup(variant.getRepetition())
                  .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
                  .id(variant.getId().intValue())
                  .required(BINARY)
                  .named("metadata")
                  .optional(BINARY)
                  .named("value")
                  .addField(shreddedType)
                  .named(variant.getName());
            }
          }
        }
      }
    }

    return variant;
  }

  private int getFieldIndex(String[] path) {
    if (path == null || path.length == 0) {
      return -1;
    }

    // TODO: For now, we only support top-level variant fields. To support nested variants, we would
    // need to navigate the struct hierarchy
    if (path.length == 1) {
      String fieldName = path[0];
      for (int i = 0; i < sparkSchema.fields().length; i++) {
        if (sparkSchema.fields()[i].name().equals(fieldName)) {
          return i;
        }
      }
    } else {
      LOG.warn(
          "Nested variant fields are not yet supported for schema inference. Path: {}",
          String.join(".", path));
    }

    return -1;
  }
}
