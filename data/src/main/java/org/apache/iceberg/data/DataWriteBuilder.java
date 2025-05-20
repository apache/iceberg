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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.DataWriter;

/**
 * A specialized builder for creating data content file writers.
 *
 * <p>This builder extends the generic {@link ContentFileWriteBuilder} interface with functionality
 * specific to creating {@link DataWriter} instances. Data writers produce table content files
 * containing actual data records stored in an Iceberg table, configured according to the table's
 * schema and partition specification.
 *
 * @param <B> the concrete builder type for method chaining
 * @param <E> engine-specific schema type required by the writer for data conversion
 */
public interface DataWriteBuilder<B extends DataWriteBuilder<B, E>, E>
    extends ContentFileWriteBuilder<B, E> {
  /**
   * Creates a data file writer configured with the current builder settings.
   *
   * <p>The returned {@link DataWriter} produces files that conform to the Iceberg table format,
   * generating proper {@link DataFile} metadata on completion. The writer accepts input records
   * matching the engine schema specified via {@link #dataSchema(Object)} and converts them to the
   * target Iceberg schema specified via {@link #schema(Schema)} for writing.
   *
   * @param <D> the type of data records the writer will accept
   * @return a fully configured {@link DataWriter} instance
   * @throws IOException if the writer cannot be created due to I/O errors
   */
  <D> DataWriter<D> dataWriter() throws IOException;
}
