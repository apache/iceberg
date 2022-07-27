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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.vectorized.VectorizedReaderBuilder;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;

public class VectorizedSparkParquetReaders {

  private VectorizedSparkParquetReaders() {}

  public static ColumnarBatchReader buildReader(
      Schema expectedSchema, MessageType fileSchema, boolean setArrowValidityVector) {
    return buildReader(expectedSchema, fileSchema, setArrowValidityVector, Maps.newHashMap());
  }

  public static ColumnarBatchReader buildReader(
      Schema expectedSchema,
      MessageType fileSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant) {
    return (ColumnarBatchReader)
        TypeWithSchemaVisitor.visit(
            expectedSchema.asStruct(),
            fileSchema,
            new VectorizedReaderBuilder(
                expectedSchema,
                fileSchema,
                setArrowValidityVector,
                idToConstant,
                ColumnarBatchReader::new));
  }

  public static ColumnarBatchReader buildReader(
      Schema expectedSchema,
      MessageType fileSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant,
      DeleteFilter<InternalRow> deleteFilter) {
    return (ColumnarBatchReader)
        TypeWithSchemaVisitor.visit(
            expectedSchema.asStruct(),
            fileSchema,
            new ReaderBuilder(
                expectedSchema,
                fileSchema,
                setArrowValidityVector,
                idToConstant,
                ColumnarBatchReader::new,
                deleteFilter));
  }

  private static class ReaderBuilder extends VectorizedReaderBuilder {
    private final DeleteFilter<InternalRow> deleteFilter;

    ReaderBuilder(
        Schema expectedSchema,
        MessageType parquetSchema,
        boolean setArrowValidityVector,
        Map<Integer, ?> idToConstant,
        Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
        DeleteFilter<InternalRow> deleteFilter) {
      super(expectedSchema, parquetSchema, setArrowValidityVector, idToConstant, readerFactory);
      this.deleteFilter = deleteFilter;
    }

    @Override
    protected VectorizedReader<?> vectorizedReader(List<VectorizedReader<?>> reorderedFields) {
      VectorizedReader<?> reader = super.vectorizedReader(reorderedFields);
      if (deleteFilter != null) {
        ((ColumnarBatchReader) reader).setDeleteFilter(deleteFilter);
      }
      return reader;
    }
  }
}
