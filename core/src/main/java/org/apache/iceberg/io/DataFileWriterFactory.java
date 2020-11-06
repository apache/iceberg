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

import org.apache.iceberg.ContentFileWriter;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFileWriter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;

/**
 * TODO: This factory would be removed once we've replaced by using {@link DeltaWriterFactory} in the upper layer.
 */
public class DataFileWriterFactory<D> implements ContentFileWriterFactory<DataFile, D> {
  private final FileAppenderFactory<D> appenderFactory;
  private final PartitionSpec spec;

  public DataFileWriterFactory(FileAppenderFactory<D> appenderFactory, PartitionSpec spec) {
    this.appenderFactory = appenderFactory;
    this.spec = spec;
  }

  @Override
  public ContentFileWriter<DataFile, D> createWriter(PartitionKey partitionKey,
                                                     EncryptedOutputFile outputFile,
                                                     FileFormat fileFormat) {
    FileAppender<D> appender = appenderFactory.newAppender(outputFile.encryptingOutputFile(), fileFormat);
    return new DataFileWriter<>(appender, fileFormat, outputFile.encryptingOutputFile().location(), partitionKey,
        spec, outputFile.keyMetadata());
  }
}
