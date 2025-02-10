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
package org.apache.iceberg.io.datafile;

import java.nio.ByteBuffer;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;

/**
 * Builder API for reading Iceberg data files.
 *
 * @param <T> the type of the builder for chaining
 */
public interface ReaderBuilder<T extends ReaderBuilder<T>> {
  /**
   * Restricts the read to the given range: [start, start + length).
   *
   * @param newStart the start position for this read
   * @param newLength the length of the range this read should scan
   */
  T split(long newStart, long newLength);

  /** Read only the given columns. */
  T project(Schema newSchema);

  /** Sets the reader to case-sensitive when matching column names. */
  T caseInsensitive();

  T caseSensitive(boolean newCaseSensitive);

  /** Enables record filtering. */
  T filterRecords(boolean newFilterRecords);

  /**
   * Pushes down the {@link Expression} filter for the reader to prevent reading unnecessary
   * records.
   */
  T filter(Expression newFilter);

  /** Sets configuration key/value pairs for the reader. */
  T set(String key, String value);

  /** Enables reusing the containers returned by the reader. Decreases pressure on GC. */
  T reuseContainers();

  T reuseContainers(boolean newReuseContainers);

  /** Sets the batch size for vectorized readers. */
  T recordsPerBatch(int numRowsPerBatch);

  /** Sets a mapping from external schema names to Iceberg type IDs. */
  T withNameMapping(NameMapping newNameMapping);

  T withFileEncryptionKey(ByteBuffer encryptionKey);

  T withAADPrefix(ByteBuffer aadPrefix);

  <D> CloseableIterable<D> build();
}
