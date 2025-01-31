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

import java.io.IOException;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Base implementation for the {@link FileFormatReadBuilder} which handles the common attributes for
 * the builders.
 *
 * @param <T> the type of the builder which is needed so method chaining is available for the
 *     builder
 */
public class FileFormatDataWriterBuilderBase<T extends FileFormatDataWriterBuilderBase<T>>
    extends FileFormatWriterBuilderBase<T> implements FileFormatDataWriterBuilder<T> {

  public FileFormatDataWriterBuilderBase(
      FileFormatAppenderBuilder<?> appenderBuilder, FileFormat format) {
    super(appenderBuilder, format);
  }

  @Override
  public <T> DataWriter<T> build() throws IOException {
    Preconditions.checkArgument(spec() != null, "Cannot create data writer without spec");
    Preconditions.checkArgument(
        spec().isUnpartitioned() || partition() != null,
        "Partition must not be null when creating data writer for partitioned spec");

    FileAppender<T> fileAppender = appenderBuilder().build();
    return new DataWriter<>(
        fileAppender,
        format(),
        appenderBuilder().location(),
        spec(),
        partition(),
        keyMetadata(),
        sortOrder());
  }
}
