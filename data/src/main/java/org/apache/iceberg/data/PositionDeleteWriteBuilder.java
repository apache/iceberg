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
import org.apache.iceberg.deletes.PositionDeleteWriter;

/**
 * Builder for generating an {@link PositionDeleteWriter}.
 *
 * @param <B> type of the builder
 * @param <E> engine-specific schema of the input records used for appender initialization
 */
public interface PositionDeleteWriteBuilder<B extends PositionDeleteWriteBuilder<B, E>, E>
    extends ContentFileWriteBuilderBase<B, E> {
  /** Sets the row schema for the delete writers. */
  B rowSchema(Schema newSchema);

  /**
   * Creates a writer which generates a position {@link DeleteFile} based on the configurations. The
   * writer will expect {@link org.apache.iceberg.deletes.PositionDelete} records. If {@link
   * #rowSchema(Schema)} is set then the positional delete records should contain deleted rows
   * specified by the {@link #dataSchema(Object)} (Object)}. The provided engine schema should be
   * convertible to the Iceberg schema defined by {@link #rowSchema(Schema)} (Schema)}.
   *
   * @param <D> the type of data that the writer will handle
   * @return a {@link PositionDeleteWriter} instance configured with the specified settings
   * @throws IOException if an I/O error occurs during the creation of the writer
   */
  <D> PositionDeleteWriter<D> positionDeleteWriter() throws IOException;
}
