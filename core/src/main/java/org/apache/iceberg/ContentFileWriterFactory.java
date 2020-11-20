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

package org.apache.iceberg;

import org.apache.iceberg.encryption.EncryptedOutputFile;

/**
 * Factory to create a new {@link ContentFileWriter} to write INSERT or DELETE records.
 *
 * @param <T> Content file type, it's either {@link DataFile} or {@link DeleteFile}.
 * @param <R> data type of the rows to write.
 */
public interface ContentFileWriterFactory<T, R> {

  /**
   * Create a new {@link ContentFileWriter}
   *
   * @param partitionKey an partition key to indicate which partition the rows will be written. Null if it's
   *                     unpartitioned.
   * @param outputFile   an OutputFile used to create an output stream.
   * @param fileFormat   File format.
   * @return a newly created {@link ContentFileWriter}
   */
  ContentFileWriter<T, R> createWriter(PartitionKey partitionKey, EncryptedOutputFile outputFile,
                                       FileFormat fileFormat);
}
