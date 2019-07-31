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
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizedSparkParquetReaders {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedSparkParquetReaders.class);

  private VectorizedSparkParquetReaders() {
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<ColumnarBatch> buildReader(
      Schema tableSchema,
      Schema expectedSchema,
      MessageType fileSchema) {

    return buildReader(tableSchema, expectedSchema, fileSchema,
        VectorizedParquetValueReaders.VectorReader.DEFAULT_NUM_ROWS_IN_BATCH);
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<ColumnarBatch> buildReader(
      Schema tableSchema,
      Schema expectedSchema,
      MessageType fileSchema,
      Integer recordsPerBatch) {

    LOG.info("=> [VectorizedSparkParquetReaders] recordsPerBatch = " + recordsPerBatch);
    return (ParquetValueReader<ColumnarBatch>)
        TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
            new ReadBuilder(tableSchema, expectedSchema, fileSchema, recordsPerBatch));
  }

  private static class ReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
    private final MessageType parquetSchema;
    private final Schema projectedIcebergSchema;
    private final Schema tableIcebergSchema;
    private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;
    private final BufferAllocator rootAllocator;
    private final int recordsPerBatch;

    ReadBuilder(Schema tableSchema, Schema projectedIcebergSchema, MessageType parquetSchema, int recordsPerBatch) {
      this.parquetSchema = parquetSchema;
      this.tableIcebergSchema = tableSchema;
      this.projectedIcebergSchema = projectedIcebergSchema;
      this.arrowSchema = ArrowSchemaUtil.convert(projectedIcebergSchema);
      this.recordsPerBatch = recordsPerBatch;
      // this.rootAllocator = new RootAllocator(Long.MAX_VALUE);
      this.rootAllocator = ArrowUtils.rootAllocator().newChildAllocator("VectorizedReadBuilder",
          0, Long.MAX_VALUE);
      LOG.info("=> [ReadBuilder] recordsPerBatch = " + this.recordsPerBatch);
    }

    @Override
    public ParquetValueReader<?> message(Types.StructType expected, MessageType message,
        List<ParquetValueReader<?>> fieldReaders) {
      return struct(expected, message.asGroupType(), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(Types.StructType expected, GroupType struct,
        List<ParquetValueReader<?>> fieldReaders) {

      // this works on struct fields and the root iceberg schema which itself is a struct.

      // match the expected struct's order
      Map<Integer, ParquetValueReader<FieldVector>> readersById = Maps.newHashMap();
      Map<Integer, Type> typesById = Maps.newHashMap();
      List<Type> fields = struct.getFields();

      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = parquetSchema.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
        int id = fieldType.getId().intValue();
        // Todo: figure out optional vield reading for vectorized reading
        // readersById.put(id, (ParquetValueReader<FieldVector>)ParquetValueReaders.
        //     option(fieldType, fieldD, fieldReaders.get(i)));

        readersById.put(id, (ParquetValueReader<FieldVector>) fieldReaders.get(i));
        typesById.put(id, fieldType);
      }

      List<Types.NestedField> icebergFields = expected != null ?
          expected.fields() : ImmutableList.of();

      List<ParquetValueReader<FieldVector>> reorderedFields = Lists.newArrayListWithExpectedSize(
          icebergFields.size());

      List<Type> types = Lists.newArrayListWithExpectedSize(icebergFields.size());

      for (Types.NestedField field : icebergFields) {
        int id = field.fieldId();
        ParquetValueReader<FieldVector> reader = readersById.get(id);
        if (reader != null) {
          reorderedFields.add(reader);
          types.add(typesById.get(id));
        } else {
          reorderedFields.add(ParquetValueReaders.nulls());
          types.add(null);
        }
      }

      return new ParquetValueReaders.ColumnarBatchReader(types, expected, reorderedFields);
    }


    @Override
    public ParquetValueReader<?> primitive(org.apache.iceberg.types.Type.PrimitiveType expected,
        PrimitiveType primitive) {

      // Create arrow vector for this field
      int parquetFieldId = primitive.getId().intValue();
      ColumnDescriptor desc = parquetSchema.getColumnDescription(currentPath());
      Types.NestedField icebergField = tableIcebergSchema.findField(parquetFieldId);
      // int fieldD = parquetSchema.getMaxDefinitionLevel(path(primitive.getName())) - 1;
      // Field field = ArrowSchemaUtil.convert(projectedIcebergSchema.findField(parquetFieldId));
      // FieldVector vec = field.createVector(rootAllocator);

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return new VectorizedParquetValueReaders.StringReader(desc, icebergField, rootAllocator, recordsPerBatch);
          case INT_8:
          case INT_16:
          case INT_32:
            return new VectorizedParquetValueReaders.IntegerReader(desc, icebergField, rootAllocator, recordsPerBatch);
            // if (expected != null && expected.typeId() == Types.LongType.get().typeId()) {
            //   return new ParquetValueReaders.IntAsLongReader(desc);
            // } else {
            //   return new ParquetValueReaders.UnboxedReader(desc);
            // }
          case DATE:
            return new VectorizedParquetValueReaders.DateReader(desc, icebergField, rootAllocator, recordsPerBatch);
          case INT_64:
            return new VectorizedParquetValueReaders.LongReader(desc, icebergField, rootAllocator, recordsPerBatch);
          case TIMESTAMP_MICROS:
            return new VectorizedParquetValueReaders.TimestampMicroReader(desc, icebergField,
                rootAllocator, recordsPerBatch);
          case TIMESTAMP_MILLIS:
            return new VectorizedParquetValueReaders.TimestampMillisReader(desc, icebergField,
                rootAllocator, recordsPerBatch);
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new VectorizedParquetValueReaders.BinaryDecimalReader(desc, icebergField, rootAllocator,
                    decimal.getPrecision(),
                    decimal.getScale(), recordsPerBatch);
              case INT64:
                return new VectorizedParquetValueReaders.LongDecimalReader(desc, icebergField, rootAllocator,
                    decimal.getPrecision(),
                    decimal.getScale(), recordsPerBatch);
              case INT32:
                return new VectorizedParquetValueReaders.IntegerDecimalReader(desc, icebergField, rootAllocator,
                    decimal.getPrecision(),
                    decimal.getScale(), recordsPerBatch);
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return new VectorizedParquetValueReaders.BinaryReader(desc, icebergField, rootAllocator, recordsPerBatch);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return new VectorizedParquetValueReaders.BinaryReader(desc, icebergField, rootAllocator, recordsPerBatch);
        case INT32:
          return new VectorizedParquetValueReaders.IntegerReader(desc, icebergField, rootAllocator, recordsPerBatch);
        case FLOAT:
          return new VectorizedParquetValueReaders.FloatReader(desc, icebergField, rootAllocator, recordsPerBatch);
          // if (expected != null && expected.typeId() == org.apache.iceberg.types.Type.TypeID.DOUBLE) {
          //   return new ParquetValueReaders.FloatAsDoubleReader(desc);
          // } else {
          //   return new ParquetValueReaders.UnboxedReader<>(desc);
          // }
        case BOOLEAN:
          return new VectorizedParquetValueReaders.BooleanReader(desc, icebergField, rootAllocator, recordsPerBatch);
        case INT64:
          return new VectorizedParquetValueReaders.LongReader(desc, icebergField, rootAllocator, recordsPerBatch);
        case DOUBLE:
          return new VectorizedParquetValueReaders.DoubleReader(desc, icebergField, rootAllocator, recordsPerBatch);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
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
