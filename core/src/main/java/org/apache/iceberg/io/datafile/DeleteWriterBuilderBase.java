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
package org.apache.iceberg.io.datafile;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

/**
 * Base implementation for the {@link EqualityDeleteWriterBuilder} which handles the common
 * attributes for the builders. FileFormat implementations should extend this for creating their own
 * equality delete writer builders. Uses an embedded {@link AppenderBuilder} to actually write the
 * records.
 *
 * @param <T> the type of the builder for chaining
 */
abstract class DeleteWriterBuilderBase<T extends DeleteWriterBuilderBase<T>>
    extends FileWriterBuilderBase<T> implements DeleteWriterBuilder<T> {
  private Schema rowSchema = null;

  DeleteWriterBuilderBase(AppenderBuilder<?> appenderBuilder, FileFormat format) {
    super(appenderBuilder, format);
  }

  @Override
  public T forTable(Table table) {
    this.rowSchema = table.schema();
    return super.forTable(table);
  }

  @Override
  public T rowSchema(Schema schema) {
    this.rowSchema = schema;
    return (T) this;
  }

  protected Schema rowSchema() {
    return rowSchema;
  }
}
