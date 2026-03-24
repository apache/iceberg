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
package org.apache.iceberg.lance.spark;

import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.lance.LanceFormatModel;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Registers Lance format models for Spark's InternalRow and ColumnarBatch data types.
 *
 * <p>This class is auto-discovered by {@link FormatModelRegistry} via reflection when the
 * iceberg-spark jar and iceberg-lance jar are both on the classpath.
 */
public class SparkLanceFormatModels {
  public static void register() {
    // ColumnarBatch — vectorized, near-zero-copy via ArrowColumnVector
    FormatModelRegistry.register(
        LanceFormatModel.create(
            ColumnarBatch.class,
            StructType.class,
            null, // no writer for ColumnarBatch (read-only vectorized path)
            (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
                SparkLanceColumnarReader.buildReader(icebergSchema, idToConstant)));

    // InternalRow — row-based reads and writes
    FormatModelRegistry.register(
        LanceFormatModel.create(
            InternalRow.class,
            StructType.class,
            (icebergSchema, arrowSchema, engineSchema) ->
                SparkLanceWriter.buildWriter(icebergSchema),
            (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
                SparkLanceRowReader.buildReader(icebergSchema, idToConstant)));
  }

  private SparkLanceFormatModels() {}
}
