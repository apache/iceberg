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

import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.PositionDeleteIndexReader;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetFormatModel;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.CharSequenceMap;

public class ArrowFormatModels {
  public static void register() {
    FormatModelRegistry.register(
        ParquetFormatModel.create(
            ColumnarBatch.class,
            Object.class,
            (schema, fileSchema, engineSchema, idToConstant) ->
                ArrowReader.VectorizedCombinedScanIterator.buildReader(
                    schema,
                    fileSchema,
                    NullCheckingForGet.NULL_CHECKING_ENABLED /* setArrowValidityVector */)));

    FormatModelRegistry.registerPositionDeleteIndexReader(
        FileFormat.PARQUET, ArrowPositionDeleteIndexReader.INSTANCE);
  }

  /**
   * Adapts {@link VectorizedPositionDeleteReader} to the {@link PositionDeleteIndexReader} SPI so
   * delete loaders consult the Arrow fast path through {@link FormatModelRegistry}.
   */
  private static final class ArrowPositionDeleteIndexReader implements PositionDeleteIndexReader {
    private static final ArrowPositionDeleteIndexReader INSTANCE =
        new ArrowPositionDeleteIndexReader();

    @Override
    public PositionDeleteIndex read(
        InputFile file, CharSequence dataLocation, DeleteFile deleteFile) {
      // The SPI requires a non-null filter. VectorizedPositionDeleteReader#read also accepts
      // null as "no filter / union all rows", but that mode is intentionally a direct-API
      // optimization (e.g. for single-data-file delete files); SPI callers must pick a path
      // explicitly via readAll(...) instead.
      Preconditions.checkArgument(dataLocation != null, "Invalid data location: null");
      return VectorizedPositionDeleteReader.read(file, dataLocation, deleteFile);
    }

    @Override
    public CharSequenceMap<PositionDeleteIndex> readAll(InputFile file, DeleteFile deleteFile) {
      return VectorizedPositionDeleteReader.readAllByDataFile(file, deleteFile);
    }
  }

  private ArrowFormatModels() {}
}
