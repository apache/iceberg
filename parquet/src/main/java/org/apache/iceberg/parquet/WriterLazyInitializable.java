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
package org.apache.iceberg.parquet;

import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.schema.MessageType;

/**
 * Interface for ParquetValueWriters that need to defer initialization until they can analyze the
 * data. This is useful for scenarios like variant shredding where the schema needs to be inferred
 * from the actual data before creating the writer structures.
 *
 * <p>Writers implementing this interface can buffer initial rows and perform schema inference
 * before committing to a final Parquet schema.
 */
public interface WriterLazyInitializable {
  /**
   * Result returned by lazy initialization of a ParquetValueWriter required by ParquetWriter.
   * Contains the finalized schema and write stores after schema inference or other initialization
   * logic.
   */
  class InitializationResult {
    private final MessageType schema;
    private final ColumnChunkPageWriteStore pageStore;
    private final ColumnWriteStore writeStore;

    public InitializationResult(
        MessageType schema, ColumnChunkPageWriteStore pageStore, ColumnWriteStore writeStore) {
      this.schema = schema;
      this.pageStore = pageStore;
      this.writeStore = writeStore;
    }

    public MessageType getSchema() {
      return schema;
    }

    public ColumnChunkPageWriteStore getPageStore() {
      return pageStore;
    }

    public ColumnWriteStore getWriteStore() {
      return writeStore;
    }
  }

  /**
   * Checks if this writer still needs initialization. This will return true until the writer has
   * buffered enough data to perform initialization (e.g., schema inference).
   *
   * @return true if initialization is still needed, false if already initialized
   */
  boolean needsInitialization();

  /**
   * Performs initialization and returns the result containing updated schema and write stores. This
   * method should only be called when {@link #needsInitialization()} returns true.
   *
   * @param props Parquet properties needed for creating write stores
   * @param compressor Bytes compressor for compression
   * @param rowGroupOrdinal The ordinal number of the current row group
   * @return InitializationResult containing the finalized schema and write stores
   */
  InitializationResult initialize(
      ParquetProperties props,
      CompressionCodecFactory.BytesInputCompressor compressor,
      int rowGroupOrdinal);
}
