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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;

/**
 * A specialized builder for creating position-based delete file writers.
 *
 * <p>This builder extends the generic {@link ContentFileWriteBuilder} interface with functionality
 * specific to creating {@link PositionDeleteWriter} instances.
 *
 * <p>The builder provides methods to configure the schema for the row data that might be included
 * with the position deletes through {@link #rowSchema(Schema)}, enabling optional preservation of
 * deleted record content.
 *
 * @param <B> the concrete builder type for method chaining
 * @param <E> output schema type required by the writer for data conversion
 * @param <D> the type of data records the writer will accept
 */
public interface PositionDeleteWriteBuilder<B extends PositionDeleteWriteBuilder<B, E, D>, E, D>
    extends ContentFileWriteBuilder<B, E> {
  /**
   * Configures the schema for deleted row data to be stored in position delete files.
   *
   * <p>Position delete files can optionally store the content of deleted rows. When this schema is
   * set, the writer will expect position delete records to include the complete row data of the
   * records being deleted.
   *
   * <p>The row values provided to the writer should match the structure specified via {@link
   * #modelSchema(Object)} in the writer's native input data format. These values will be converted
   * to the Iceberg schema defined here for storage in the delete file's 'row' column.
   *
   * <p>If not configured, position delete files will contain only the path and position information
   * without preserving the deleted record content.
   *
   * @param newRowSchema the Iceberg schema defining the structure for deleted row content
   * @return this builder for method chaining
   */
  B rowSchema(Schema newRowSchema);

  /**
   * Creates a position-based delete file writer configured with the current builder settings.
   *
   * <p>The returned {@link PositionDeleteWriter} produces files that identify records to be deleted
   * by their file path and position, generating proper {@link DeleteFile} metadata on completion.
   * The writer expects {@link PositionDelete} records as input.
   *
   * @return a fully configured {@link PositionDeleteWriter} instance
   * @throws IOException if the writer cannot be created due to I/O errors
   */
  PositionDeleteWriter<D> build() throws IOException;
}
