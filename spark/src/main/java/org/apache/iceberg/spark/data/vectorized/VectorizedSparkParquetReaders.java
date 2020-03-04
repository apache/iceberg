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

package org.apache.iceberg.spark.data.vectorized;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.spark.arrow.ArrowUtils;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class VectorizedSparkParquetReaders {

  private VectorizedSparkParquetReaders() {
  }

  @SuppressWarnings("unchecked")
  public static ColumnarBatchReaders buildReader(
      Schema tableSchema,
      Schema expectedSchema,
      MessageType fileSchema,
      Integer recordsPerBatch) {
    return (ColumnarBatchReaders)
        TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
            new VectorizedReaderBuilder(tableSchema, expectedSchema, fileSchema, recordsPerBatch));
  }

  private static class VectorizedReaderBuilder extends TypeWithSchemaVisitor<VectorizedReader> {
    private final MessageType parquetSchema;
    private final Schema tableIcebergSchema;
    private final BufferAllocator rootAllocator;
    private final int batchSize;

    VectorizedReaderBuilder(
        Schema tableSchema,
        Schema projectedIcebergSchema,
        MessageType parquetSchema,
        int bSize) {
      this.parquetSchema = parquetSchema;
      this.tableIcebergSchema = tableSchema;
      this.batchSize = bSize;
      this.rootAllocator = ArrowUtils.instance().rootAllocator()
          .newChildAllocator("VectorizedReadBuilder", 0, Long.MAX_VALUE);
    }

    @Override
    public VectorizedReader message(
            Types.StructType expected, MessageType message,
            List<VectorizedReader> fieldReaders) {
      return struct(expected, message.asGroupType(), fieldReaders);
    }

    @Override
    public VectorizedReader struct(
            Types.StructType expected, GroupType struct,
            List<VectorizedReader> fieldReaders) {

      Map<Integer, VectorizedReader> readersById = Maps.newHashMap();
      List<Type> fields = struct.getFields();

      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int id = fieldType.getId().intValue();
        readersById.put(id, fieldReaders.get(i));
      }

      List<Types.NestedField> icebergFields = expected != null ?
          expected.fields() : ImmutableList.of();

      List<VectorizedReader> reorderedFields = Lists.newArrayListWithExpectedSize(
          icebergFields.size());

      for (Types.NestedField field : icebergFields) {
        int id = field.fieldId();
        VectorizedReader reader = readersById.get(id);
        if (reader != null) {
          reorderedFields.add(reader);
        } else {
          reorderedFields.add(VectorizedArrowReader.NULL_VALUES_READER);
        }
      }
      return new ColumnarBatchReaders(reorderedFields, batchSize);
    }

    @Override
    public VectorizedReader primitive(
        org.apache.iceberg.types.Type.PrimitiveType expected,
        PrimitiveType primitive) {

      // Create arrow vector for this field
      int parquetFieldId = primitive.getId().intValue();
      ColumnDescriptor desc = parquetSchema.getColumnDescription(currentPath());
      // Nested types not yet supported for vectorized reads
      if (desc.getMaxRepetitionLevel() > 0) {
        return null;
      }
      Types.NestedField icebergField = tableIcebergSchema.findField(parquetFieldId);
      return new VectorizedArrowReader(desc, icebergField, rootAllocator,
          batchSize, /* setArrowValidityVector */ false);
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

    protected MessageType type() {
      return parquetSchema;
    }
  }
}
