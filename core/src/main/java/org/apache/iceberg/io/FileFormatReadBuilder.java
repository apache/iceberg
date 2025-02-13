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
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.mapping.NameMapping;

/**
 * Interface used for configuring and creating readers for different {@link
 * org.apache.iceberg.FileFormat}s.
 *
 * @param <T> the type of the builder which is needed so method chaining is available for the
 *     builder
 */
public interface FileFormatReadBuilder<T extends FileFormatReadBuilder<T>> {
  /**
   * Restricts the read to the given range: [start, start + length).
   *
   * @param newStart the start position for this read
   * @param newLength the length of the range this read should scan
   * @return this builder for method chaining
   */
  T split(long newStart, long newLength);

  T project(Schema newSchema);

  T caseInsensitive();

  T caseSensitive(boolean newCaseSensitive);

  T filterRecords(boolean newFilterRecords);

  T filter(Expression newFilter);

  T set(String key, String value);

  T reuseContainers();

  T reuseContainers(boolean newReuseContainers);

  T recordsPerBatch(int numRowsPerBatch);

  T withNameMapping(NameMapping newNameMapping);

  T withFileEncryptionKey(ByteBuffer encryptionKey);

  T withAADPrefix(ByteBuffer aadPrefix);

  <D> CloseableIterable<D> build();
}
