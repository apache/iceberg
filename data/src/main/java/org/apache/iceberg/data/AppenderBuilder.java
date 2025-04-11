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
import org.apache.iceberg.io.FileAppender;

/**
 * Builder for generating a {@link FileAppender}.
 *
 * @param <B> type of the builder
 * @param <E> engine specific schema of the input records used for appender initialization
 */
public interface AppenderBuilder<B extends AppenderBuilder<B, E>, E>
    extends WriterBuilderBase<B, E> {
  /**
   * Creates a {@link FileAppender} based on the configurations set. The appender will expect inputs
   * defined by the {@link #engineSchema(Object)}} which should match the Iceberg schema defined by
   * {@link #schema(Schema)}.
   *
   * @param <D> the type of data that the appender will handle
   * @return a {@link FileAppender} instance configured with the specified settings
   * @throws IOException if an I/O error occurs during the creation of the appender
   */
  <D> FileAppender<D> appender() throws IOException;
}
