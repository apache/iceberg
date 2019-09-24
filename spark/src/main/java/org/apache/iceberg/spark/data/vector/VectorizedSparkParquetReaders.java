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

package org.apache.iceberg.spark.data.vector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.parquet.BatchedReader;
import org.apache.iceberg.parquet.ColumnarBatchReader;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorReader;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizedSparkParquetReaders {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedSparkParquetReaders.class);

  private VectorizedSparkParquetReaders() {
  }

  @SuppressWarnings("unchecked")
  public static ColumnarBatchReader buildReader(
      Schema tableSchema,
      Schema expectedSchema,
      MessageType fileSchema) {

    return buildReader(tableSchema, expectedSchema, fileSchema,
        VectorReader.DEFAULT_BATCH_SIZE);
  }

  @SuppressWarnings("unchecked")
  public static ColumnarBatchReader buildReader(
      Schema tableSchema,
      Schema expectedSchema,
      MessageType fileSchema,
      Integer recordsPerBatch) {

    LOG.info("=> [VectorizedSparkParquetReaders] recordsPerBatch = {}", recordsPerBatch);
    return (ColumnarBatchReader)
        TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
            new ReadBuilderBatched(tableSchema, expectedSchema, fileSchema, recordsPerBatch));
  }

  private static class ReadBuilderBatched extends TypeWithSchemaVisitor<BatchedReader> {
    private final MessageType parquetSchema;
    private final Schema projectedIcebergSchema;
    private final Schema tableIcebergSchema;
    private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;
    private final BufferAllocator rootAllocator;
    private final int recordsPerBatch;

    ReadBuilderBatched(
        Schema tableSchema,
        Schema projectedIcebergSchema,
        MessageType parquetSchema,
        int recordsPerBatch) {
      this.parquetSchema = parquetSchema;
      this.tableIcebergSchema = tableSchema;
      this.projectedIcebergSchema = projectedIcebergSchema;
      this.arrowSchema = ArrowSchemaUtil.convert(projectedIcebergSchema);
      this.recordsPerBatch = recordsPerBatch;
      this.rootAllocator = ArrowUtils.rootAllocator().newChildAllocator("VectorizedReadBuilder", 0, Long.MAX_VALUE);
      LOG.info("=> [ReadBuilder] recordsPerBatch = {}", this.recordsPerBatch);
    }

    @Override
    public BatchedReader message(
        Types.StructType expected, MessageType message,
        List<BatchedReader> fieldReaders) {
      return struct(expected, message.asGroupType(), fieldReaders);
    }

    @Override
    public BatchedReader struct(
        Types.StructType expected, GroupType struct,
        List<BatchedReader> fieldReaders) {

      // this works on struct fields and the root iceberg schema which itself is a struct.

      // match the expected struct's order
      Map<Integer, BatchedReader> readersById = Maps.newHashMap();
      Map<Integer, Type> typesById = Maps.newHashMap();
      List<Type> fields = struct.getFields();

      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int id = fieldType.getId().intValue();
        readersById.put(id, fieldReaders.get(i));
        typesById.put(id, fieldType);
      }

      List<Types.NestedField> icebergFields = expected != null ?
          expected.fields() : ImmutableList.of();

      List<BatchedReader> reorderedFields = Lists.newArrayListWithExpectedSize(
          icebergFields.size());

      List<Type> types = Lists.newArrayListWithExpectedSize(icebergFields.size());

      for (Types.NestedField field : icebergFields) {
        int id = field.fieldId();
        BatchedReader reader = readersById.get(id);
        if (reader != null) {
          reorderedFields.add(reader);
          types.add(typesById.get(id));
        } else {
          reorderedFields.add(null); // anjali-todo We need a NullVectorReader
          types.add(null);
        }
      }

      return new ColumnarBatchReader(types, expected, reorderedFields);
    }

    @Override
    public BatchedReader primitive(
        org.apache.iceberg.types.Type.PrimitiveType expected,
        PrimitiveType primitive) {

      // Create arrow vector for this field
      int parquetFieldId = primitive.getId().intValue();
      ColumnDescriptor desc = parquetSchema.getColumnDescription(currentPath());
      Types.NestedField icebergField = tableIcebergSchema.findField(parquetFieldId);
      return new VectorReader(desc, icebergField, rootAllocator, recordsPerBatch);
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

    protected String[] path(String name) {
      String[] path = new String[fieldNames.size() + 1];
      path[fieldNames.size()] = name;

      if (!fieldNames.isEmpty()) {
        Iterator<String> iter = fieldNames.descendingIterator();
        for (int i = 0; iter.hasNext(); i += 1) {
          path[i] = iter.next();
        }
      }

      return path;
    }
  }
}
