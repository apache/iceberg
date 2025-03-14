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

/**
 * Builder API for reading Iceberg data files.
 *
 * @param <R> type of the reader
 * @param <F> type of the records which are filtered by {@link DeleteFilter}
 */
public interface ReadBuilder<R extends ReadBuilder<R, F>, F> {
  /**
   * Restricts the read to the given range: [start, start + length).
   *
   * @param newStart the start position for this read
   * @param newLength the length of the range this read should scan
   */
  R split(long newStart, long newLength);

  /** Read only the given columns. */
  R project(Schema newSchema);

  /**
   * Sets the reader to case-sensitive when matching column names. Readers might decide not to
   * implement this feature. The default is behavior is case-sensitive.
   */
  default R caseInsensitive() {
    return caseSensitive(false);
  }

  default R caseSensitive(boolean newCaseSensitive) {
    // Just ignore case sensitivity if not available
    return (R) this;
  }

  /**
   * Enables record filtering. Some readers might not be able to do reader side filtering. In this
   * case the reader might decide on returning every row. It is the caller's responsibility to apply
   * the filter again.
   */
  default R filterRecords(boolean newFilterRecords) {
    // Skip filtering if not available
    return (R) this;
  }

  /**
   * Pushes down the {@link Expression} filter for the reader to prevent reading unnecessary
   * records. Some readers might not be able to filter some part of the exception. In this case the
   * reader might return unfiltered or partially filtered rows. It is the caller's responsibility to
   * apply the filter again.
   */
  default R filter(Expression newFilter) {
    // Skip filtering if not available
    return (R) this;
  }

  /**
   * Sets configuration key/value pairs for the reader. Reader builders could ignore configuration
   * keys not known for them.
   */
  R set(String key, String value);

  /** Enables reusing the containers returned by the reader. Decreases pressure on GC. */
  default R reuseContainers() {
    return reuseContainers(true);
  }

  /**
   * Reusing the containers returned by the reader decreases pressure on GC. Readers could decide to
   * ignore the user provided setting if is not supported by them.
   */
  R reuseContainers(boolean newReuseContainers);

  /** Sets the batch size for vectorized readers. */
  default R recordsPerBatch(int numRowsPerBatch) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Accessors for constant field values. Used for calculating values it the result which are coming
   * from metadata, and not coming from the data files themselves.
   */
  R idToConstant(Map<Integer, ?> newIdConstant);

  /**
   * Used for filtering out deleted records on the reader level. If delete filtering is not
   * supported by the reader then the delete filter is ignored, and unfiltered results are returned.
   * It is the caller's responsibility to apply the filter again.
   */
  default R withDeleteFilter(DeleteFilter<F> newDeleteFilter) {
    // Skip filtering if not available
    return (R) this;
  }

  /** Sets a mapping from external schema names to Iceberg type IDs. */
  R withNameMapping(NameMapping newNameMapping);

  /**
   * Sets the file encryption key used for reading the file. If encryption is not supported by the
   * reader then an exception should be thrown.
   */
  default R withFileEncryptionKey(ByteBuffer encryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Sets the additional authentication data prefix for encryption. If encryption is not supported
   * by the reader then an exception should be thrown.
   */
  default R withAADPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  /** Builds the reader. */
  <D> CloseableIterable<D> build();
}
