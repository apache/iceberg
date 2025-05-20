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
 * @param <E> engine-specific schema type required by the writer for data conversion
 */
public interface PositionDeleteWriteBuilder<B extends PositionDeleteWriteBuilder<B, E>, E>
    extends ContentFileWriteBuilder<B, E> {
  /** Sets the row schema for the delete writers. */
  B rowSchema(Schema newSchema);

  /**
   * Creates a position-based delete file writer configured with the current builder settings.
   *
   * <p>The returned {@link PositionDeleteWriter} produces files that identify records to be deleted
   * by their file path and position, generating proper {@link DeleteFile} metadata on completion.
   * The writer expects {@link PositionDelete} records as input.
   *
   * <p>If {@link #rowSchema(Schema)} is configured, the position delete records should include the
   * content of the deleted rows. These row values should match the engine schema specified via
   * {@link #dataSchema(Object)} and will be converted to the target Iceberg schema defined by
   * {@link #rowSchema(Schema)}.
   *
   * @param <D> the type of position delete records the writer will accept
   * @return a fully configured {@link PositionDeleteWriter} instance
   * @throws IOException if the writer cannot be created due to I/O errors
   */
  <D> PositionDeleteWriter<D> positionDeleteWriter() throws IOException;
}
