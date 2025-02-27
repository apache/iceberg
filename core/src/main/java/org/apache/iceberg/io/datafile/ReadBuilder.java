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
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;

/** Builder API for reading Iceberg data files. */
public interface ReadBuilder {
  /**
   * Restricts the read to the given range: [start, start + length).
   *
   * @param newStart the start position for this read
   * @param newLength the length of the range this read should scan
   */
  ReadBuilder split(long newStart, long newLength);

  /** Read only the given columns. */
  ReadBuilder project(Schema newSchema);

  /** Sets the reader to case-sensitive when matching column names. */
  default ReadBuilder caseInsensitive() {
    return caseSensitive(false);
  }

  default ReadBuilder caseSensitive(boolean newCaseSensitive) {
    // Just ignore case sensitivity if not available
    return this;
  }

  /** Enables record filtering. */
  default ReadBuilder filterRecords(boolean newFilterRecords) {
    // Skip filtering if not available
    return this;
  }

  /**
   * Pushes down the {@link Expression} filter for the reader to prevent reading unnecessary
   * records.
   */
  default ReadBuilder filter(Expression newFilter) {
    // Skip filtering if not available
    return this;
  }

  /** Sets configuration key/value pairs for the reader. */
  default ReadBuilder set(String key, String value) {
    throw new UnsupportedOperationException("Not supported");
  }

  /** Enables reusing the containers returned by the reader. Decreases pressure on GC. */
  default ReadBuilder reuseContainers() {
    return reuseContainers(true);
  }

  ReadBuilder reuseContainers(boolean newReuseContainers);

  /** Sets the batch size for vectorized readers. */
  default ReadBuilder recordsPerBatch(int numRowsPerBatch) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Accessors for constant field values. Used for returning values not coming from the data files.
   */
  ReadBuilder idToConstant(Map<Integer, ?> newIdConstant);

  /** Used for filtering out deleted records on the reader level. */
  default <F> ReadBuilder withDeleteFilter(DeleteFilter<F> newDeleteFilter) {
    throw new UnsupportedOperationException("Not supported");
  }

  /** Sets a mapping from external schema names to Iceberg type IDs. */
  ReadBuilder withNameMapping(NameMapping newNameMapping);

  /** Sets the file encryption key used for reading the file. */
  default ReadBuilder withFileEncryptionKey(ByteBuffer encryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  /** Sets the additional authentication data prefix for encryption. */
  default ReadBuilder withAADPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  /** Builds the reader. */
  <D> CloseableIterable<D> build();
}
