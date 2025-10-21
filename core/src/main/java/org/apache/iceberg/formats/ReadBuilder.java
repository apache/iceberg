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
package org.apache.iceberg.formats;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;

/**
 * Builder interface for creating file readers across supported data file formats. The {@link
 * FormatModel} implementations provides appropriate {@link ReadBuilder} instances
 *
 * <p>The {@link ReadBuilder} follows the builder pattern to configure and create {@link
 * CloseableIterable} instances that read data from source files. Configuration options include
 * schema projection, predicate filtering, record batching, and encryption settings.
 *
 * <p>This interface is directly exposed to users for parameterizing readers.
 */
public interface ReadBuilder {
  /**
   * Restricts the read to the given range: [start, start + length).
   *
   * @param newStart the start position for this read
   * @param newLength the length of the range this read should scan
   */
  ReadBuilder split(long newStart, long newLength);

  /** Set the projection schema. */
  ReadBuilder project(Schema schema);

  /**
   * Configures whether filtering should be case-sensitive. If the reader supports filtering, it
   * must respect this setting. The default value is <code>true</code>.
   *
   * @param caseSensitive indicates if filtering is case-sensitive
   */
  ReadBuilder caseSensitive(boolean caseSensitive);

  /**
   * Pushes down the {@link Expression} filter for the reader to prevent reading unnecessary
   * records. Some readers might not be able to filter some part of the exception. In this case the
   * reader might return unfiltered or partially filtered rows. It is the caller's responsibility to
   * apply the filter again.
   *
   * @param filter the filter to set
   */
  ReadBuilder filter(Expression filter);

  /**
   * Set a reader configuration property which affects the reader behavior. Reader builders should
   * ignore configuration keys not known for them.
   *
   * @param key a reader config property name
   * @param value config value
   * @return this for method chaining
   */
  ReadBuilder set(String key, String value);

  /**
   * Sets multiple reader configuration properties that affect the reader behavior. Reader builders
   * should ignore configuration keys not known for them.
   *
   * @param properties reader config properties to set
   * @return this for method chaining
   */
  default ReadBuilder setAll(Map<String, String> properties) {
    properties.forEach(this::set);
    return this;
  }

  /** Enables reusing the containers returned by the reader. Decreases pressure on GC. */
  ReadBuilder reuseContainers();

  /** Sets the batch size for vectorized readers. */
  ReadBuilder recordsPerBatch(int numRowsPerBatch);

  /**
   * Contains the values in the result objects which are coming from metadata and not coming from
   * the data files themselves. The keys of the map are the column ids, the values are the constant
   * values to be used in the result.
   */
  ReadBuilder constantValues(Map<Integer, ?> constantValues);

  /** Sets a mapping from external schema names to Iceberg type IDs. */
  ReadBuilder withNameMapping(NameMapping nameMapping);

  /**
   * Sets the file encryption key used for reading the file. If the reader does not support
   * encryption, then an exception should be thrown.
   */
  ReadBuilder withFileEncryptionKey(ByteBuffer encryptionKey);

  /**
   * Sets the additional authentication data (AAD) prefix for decryption. If the reader does not
   * support decryption, then an exception should be thrown.
   */
  ReadBuilder withAADPrefix(ByteBuffer aadPrefix);

  /** Builds the reader. */
  <D> CloseableIterable<D> build();
}
