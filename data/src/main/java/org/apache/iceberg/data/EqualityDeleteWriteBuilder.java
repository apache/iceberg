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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.EqualityDeleteWriter;

/**
 * Builder for generating an {@link EqualityDeleteWriter}.
 *
 * @param <B> type of the builder
 * @param <E> engine specific schema of the input records used for appender initialization
 */
public interface EqualityDeleteWriteBuilder<B extends EqualityDeleteWriteBuilder<B, E>, E>
    extends FileWriteBuilderBase<B, E> {
  /** Sets the row schema for the delete writers. */
  B withRowSchema(Schema newSchema);

  /** Sets the equality field ids for the equality delete writer. */
  B withEqualityFieldIds(List<Integer> fieldIds);

  /** Sets the equality field ids for the equality delete writer. */
  B withEqualityFieldIds(int... fieldIds);

  /**
   * Creates a writer which generates an equality {@link DeleteFile} based on the configurations.
   * The writer will expect inputs defined by the {@link #engineSchema(Object)} which should be
   * convertible to the Iceberg schema defined by {@link #withRowSchema(Schema)}.
   *
   * @param <D> the type of data that the writer will handle
   * @return a {@link EqualityDeleteWriter} instance configured with the specified settings
   * @throws IOException if an I/O error occurs during the creation of the writer
   */
  <D> EqualityDeleteWriter<D> equalityDeleteWriter() throws IOException;
}
