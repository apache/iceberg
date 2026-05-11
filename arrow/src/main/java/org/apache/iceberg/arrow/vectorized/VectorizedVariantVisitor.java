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
package org.apache.iceberg.arrow.vectorized;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetVariantVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

/** Creates Arrow readers for a variant column's metadata and value leaves. */
class VectorizedVariantVisitor extends ParquetVariantVisitor<VectorizedReader<?>> {
  private final String[] variantGroupPath;
  private final MessageType parquetSchema;
  private final Schema icebergSchema;
  private final BufferAllocator allocator;
  private final boolean setArrowValidityVector;

  VectorizedVariantVisitor(
      String[] variantGroupPath,
      MessageType parquetSchema,
      Schema icebergSchema,
      BufferAllocator allocator,
      boolean setArrowValidityVector) {
    this.variantGroupPath = variantGroupPath;
    this.parquetSchema = parquetSchema;
    this.icebergSchema = icebergSchema;
    this.allocator = allocator;
    this.setArrowValidityVector = setArrowValidityVector;
  }

  @Override
  public VectorizedReader<?> metadata(PrimitiveType metadata) {
    ColumnDescriptor desc = resolveDescriptor(metadata);
    if (desc == null) {
      return null;
    }

    Types.NestedField field =
        Types.NestedField.required(-1, metadata.getName(), Types.BinaryType.get());
    return new VectorizedArrowReader(desc, field, allocator, setArrowValidityVector);
  }

  @Override
  public VectorizedReader<?> serialized(PrimitiveType value) {
    ColumnDescriptor desc = resolveDescriptor(value);
    if (desc == null) {
      return null;
    }

    Types.NestedField field =
        Types.NestedField.optional(-1, value.getName(), Types.BinaryType.get());
    return new VectorizedArrowReader(desc, field, allocator, setArrowValidityVector);
  }

  @Override
  public VectorizedReader<?> variant(
      GroupType variant, VectorizedReader<?> metadataResult, VectorizedReader<?> valueResult) {
    if (metadataResult instanceof VectorizedArrowReader
        && valueResult instanceof VectorizedArrowReader) {
      Types.NestedField field = findVariantField(variant);
      if (field != null) {
        return new VectorizedArrowReader.VectorizedVariantReader(
            field, (VectorizedArrowReader) metadataResult, (VectorizedArrowReader) valueResult);
      }
    }

    return null;
  }

  @Override
  public VectorizedReader<?> primitive(PrimitiveType primitive) {
    return null;
  }

  @Override
  public VectorizedReader<?> value(
      GroupType value, VectorizedReader<?> valueResult, VectorizedReader<?> typedResult) {
    return valueResult;
  }

  @Override
  public VectorizedReader<?> object(
      GroupType object, VectorizedReader<?> valueResult, List<VectorizedReader<?>> fieldResults) {
    return valueResult;
  }

  @Override
  public VectorizedReader<?> array(
      GroupType array, VectorizedReader<?> valueResult, VectorizedReader<?> elementResult) {
    return valueResult;
  }

  private ColumnDescriptor resolveDescriptor(PrimitiveType primitive) {
    // Build full column path: variant group path + primitive name
    // e.g., ["v1", "metadata"] or ["v2", "value"]
    String[] path = new String[variantGroupPath.length + 1];
    System.arraycopy(variantGroupPath, 0, path, 0, variantGroupPath.length);
    path[variantGroupPath.length] = primitive.getName();

    try {
      return parquetSchema.getColumnDescription(path);
    } catch (InvalidRecordException e) {
      return null;
    }
  }

  private Types.NestedField findVariantField(GroupType variant) {
    if (variant.getId() != null) {
      return icebergSchema.findField(variant.getId().intValue());
    }

    return null;
  }
}
