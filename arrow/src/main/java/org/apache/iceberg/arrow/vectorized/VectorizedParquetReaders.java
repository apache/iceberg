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

import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.parquet.schema.MessageType;

/**
 * Builds an {@link ArrowBatchReader}.
 */
class VectorizedParquetReaders {

  private VectorizedParquetReaders() {
  }

  /**
   * Build the {@link ArrowBatchReader} for the expected schema and file schema.
   *
   * @param expectedSchema         Expected schema of the data returned.
   * @param fileSchema             Schema of the data file.
   * @param setArrowValidityVector Indicates whether to set the validity vector in Arrow vectors.
   */
  public static ArrowBatchReader buildReader(
      Schema expectedSchema,
      MessageType fileSchema,
      boolean setArrowValidityVector) {
    return (ArrowBatchReader)
        TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
            new VectorizedReaderBuilder(
                expectedSchema, fileSchema, setArrowValidityVector, ImmutableMap.of(), ArrowBatchReader::new));
  }
}
