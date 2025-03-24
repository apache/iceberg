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
package org.apache.iceberg.io;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.mapping.NameMapping;

/**
 * Interface which should be implemented by the data file format implementations.
 *
 * @param <R> type of the reader
 */
public interface ReadBuilder<R extends ReadBuilder<R>> {
  /** The key for the batch size in case of vectorized reads. */
  String RECORDS_PER_BATCH_KEY = "iceberg.records-per-batch";

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
   * Pushes down the {@link Expression} filter for the reader to prevent reading unnecessary
   * records. Some readers might not be able to filter some part of the exception. In this case the
   * reader might return unfiltered or partially filtered rows. It is the caller's responsibility to
   * apply the filter again.
   *
   * @param newFilter the filter to set
   * @param filterCaseSensitive whether the filtering is case-sensitive or not
   */
  default R filter(Expression newFilter, boolean filterCaseSensitive) {
    // Skip filtering if not available
    return (R) this;
  }

  /**
   * Pushes down the {@link Expression} filter for the reader to prevent reading unnecessary
   * records. Some readers might not be able to filter some part of the exception. In this case the
   * reader might return unfiltered or partially filtered rows. It is the caller's responsibility to
   * apply the filter again. The default implementation sets the filter to be case-sensitive.
   *
   * @param newFilter the filter to set
   */
  default R filter(Expression newFilter) {
    return filter(newFilter, true);
  }

  /**
   * Sets configuration key/value pairs for the reader. Reader builders could ignore configuration
   * keys not known for them.
   */
  default R set(String key, String value) {
    // Skip configuration if not applicable
    return (R) this;
  }

  /** Enables reusing the containers returned by the reader. Decreases pressure on GC. */
  default R reuseContainers() {
    return reuseContainers(true);
  }

  /**
   * Reusing the containers returned by the reader decreases pressure on GC. Readers could decide to
   * ignore the user provided setting if is not supported by them.
   */
  default R reuseContainers(boolean newReuseContainers) {
    // Skip container reuse configuration if not applicable
    return (R) this;
  }

  /**
   * Accessors for constant field values. Used for calculating values in the result which are coming
   * from metadata, and not coming from the data files themselves. The keys of the map are the
   * column ids, the values are the accessors generating the values.
   */
  R constantFieldAccessors(Map<Integer, ?> constantFieldAccessors);

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
