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
package org.apache.iceberg.data;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;

/** Builder for generating a reader for the Iceberg data file. */
public class ReadBuilder {
  private final org.apache.iceberg.io.ReadBuilder<?> readBuilder;

  public ReadBuilder(org.apache.iceberg.io.ReadBuilder<?> readBuilder) {
    this.readBuilder = readBuilder;
  }

  /**
   * Restricts the read to the given range: [start, start + length).
   *
   * @param start the start position for this read
   * @param length the length of the range this read should scan
   */
  public ReadBuilder split(long start, long length) {
    readBuilder.split(start, length);
    return this;
  }

  /** Read only the given columns. */
  public ReadBuilder project(Schema schema) {
    readBuilder.project(schema);
    return this;
  }

  /**
   * Sets the reader to case-sensitive when matching column names. Readers might decide not to
   * implement this feature. The default is behavior is case-sensitive.
   */
  public ReadBuilder caseInsensitive() {
    return caseSensitive(false);
  }

  public ReadBuilder caseSensitive(boolean caseSensitive) {
    readBuilder.caseSensitive(caseSensitive);
    return this;
  }

  /**
   * Enables record filtering. Some readers might not be able to do reader side filtering. In this
   * case the reader might decide on returning every row. It is the caller's responsibility to apply
   * the filter again.
   */
  public ReadBuilder filterRecords(boolean filterRecords) {
    readBuilder.filterRecords(filterRecords);
    return this;
  }

  /**
   * Pushes down the {@link Expression} filter for the reader to prevent reading unnecessary
   * records. Some readers might not be able to filter some part of the exception. In this case the
   * reader might return unfiltered or partially filtered rows. It is the caller's responsibility to
   * apply the filter again.
   */
  public ReadBuilder filter(Expression filter) {
    readBuilder.filter(filter);
    return this;
  }

  /**
   * Sets configuration key/value pairs for the reader. Reader builders could ignore configuration
   * keys not known for them.
   */
  public ReadBuilder set(String key, String value) {
    readBuilder.set(key, value);
    return this;
  }

  /**
   * Sets configuration key/value pairs for the reader. Reader builders could ignore configuration
   * keys not known for them.
   */
  public ReadBuilder set(Map<String, String> properties) {
    properties.forEach(readBuilder::set);
    return this;
  }

  /** Enables reusing the containers returned by the reader. Decreases pressure on GC. */
  public ReadBuilder reuseContainers() {
    return reuseContainers(true);
  }

  /**
   * Reusing the containers returned by the reader decreases pressure on GC. Readers could decide to
   * ignore the user provided setting if is not supported by them.
   */
  public ReadBuilder reuseContainers(boolean reuseContainers) {
    readBuilder.reuseContainers(reuseContainers);
    return this;
  }

  /** Sets the batch size for vectorized readers. */
  public ReadBuilder recordsPerBatch(int recordsPerBatch) {
    readBuilder.recordsPerBatch(recordsPerBatch);
    return this;
  }

  /**
   * Accessors for constant field values. Used for calculating values in the result which are coming
   * from metadata, and not coming from the data files themselves. The keys of the map are the
   * column ids, the values are the accessors generating the values.
   */
  public ReadBuilder constantFieldAccessors(Map<Integer, ?> constantFieldAccessors) {
    readBuilder.constantFieldAccessors(constantFieldAccessors);
    return this;
  }

  /** Sets a mapping from external schema names to Iceberg type IDs. */
  public ReadBuilder withNameMapping(NameMapping nameMapping) {
    readBuilder.withNameMapping(nameMapping);
    return this;
  }

  /**
   * Sets the file encryption key used for reading the file. If encryption is not supported by the
   * reader then an exception should be thrown.
   */
  public ReadBuilder withFileEncryptionKey(ByteBuffer encryptionKey) {
    readBuilder.withFileEncryptionKey(encryptionKey);
    return this;
  }

  /**
   * Sets the additional authentication data prefix for encryption. If encryption is not supported
   * by the reader then an exception should be thrown.
   */
  public ReadBuilder withAADPrefix(ByteBuffer aadPrefix) {
    readBuilder.withAADPrefix(aadPrefix);
    return this;
  }

  /** Builds the reader. */
  public <D> CloseableIterable<D> build() {
    return readBuilder.build();
  }
}
