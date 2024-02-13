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
import java.util.Properties;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.arrow.vectorized.VectorizedReaderBuilder;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;

@SuppressWarnings({"VisibilityModifier", "checkstyle:HiddenField"})
public class SparkVectorizedReaderBuilder extends VectorizedReaderBuilder {
  protected DeleteFilter<InternalRow> deleteFilter;

  public SparkVectorizedReaderBuilder() {}

  public void initialize(
      Schema expectedSchema,
      MessageType parquetSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
      DeleteFilter<InternalRow> deleteFilter,
      Properties customizedVectorizationProperties) {
    this.parquetSchema = parquetSchema;
    this.icebergSchema = expectedSchema;
    this.rootAllocator =
        ArrowAllocation.rootAllocator()
            .newChildAllocator("VectorizedReadBuilder", 0, Long.MAX_VALUE);
    this.setArrowValidityVector = setArrowValidityVector;
    this.idToConstant = idToConstant;
    this.readerFactory = readerFactory;
    this.deleteFilter = deleteFilter;
  }

  protected VectorizedReader<?> vectorizedReader(List<VectorizedReader<?>> reorderedFields) {
    VectorizedReader<?> reader = readerFactory.apply(reorderedFields);
    if (deleteFilter != null) {
      ((ColumnarBatchReader) reader).setDeleteFilter(deleteFilter);
    }
    return reader;
  }
}
