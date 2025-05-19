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
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.DataWriter;

/**
 * Builder for generating a {@link DataWriter}.
 *
 * @param <B> type of the builder
 * @param <E> engine-specific schema of the input records used for appender initialization
 */
public interface DataWriteBuilder<B extends DataWriteBuilder<B, E>, E>
    extends ContentFileWriteBuilderBase<B, E> {
  /**
   * Creates a writer which generates a {@link org.apache.iceberg.DataFile} based on the
   * configurations. The data writer will expect inputs defined by the {@link #dataSchema(Object)}
   * which should be convertible to the Iceberg schema defined by {@link #schema(Schema)}.
   *
   * @param <D> the type of data that the writer will handle
   * @return a {@link DataWriter} instance configured with the specified settings
   * @throws IOException if an I/O error occurs during the creation of the writer
   */
  <D> DataWriter<D> dataWriter() throws IOException;
}
