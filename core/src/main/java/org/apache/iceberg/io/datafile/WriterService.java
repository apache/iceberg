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

import org.apache.iceberg.encryption.EncryptedOutputFile;

public interface WriterService<S> extends DataFileServiceRegistry.TypedService {
  /**
   * Provides an appender builder for the given output file which writes objects with a given input
   * type.
   *
   * @param outputFile to write to
   * @param rowType of the input records
   */
  AppenderBuilder<?> appenderBuilder(EncryptedOutputFile outputFile, S rowType);

  /**
   * Provides a data writer builder for the given output file which writes objects with a given
   * input type.
   *
   * @param outputFile to write to
   * @param rowType of the input records
   */
  DataWriterBuilder<?> dataWriterBuilder(EncryptedOutputFile outputFile, S rowType);

  /**
   * Provides an equality delete writer builder for the given output file which writes objects with
   * a given input type.
   *
   * @param outputFile to write to
   * @param rowType of the input records
   */
  EqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
      EncryptedOutputFile outputFile, S rowType);

  /**
   * Provides a positional delete writer builder for the given output file which writes objects with
   * a given input type.
   *
   * @param outputFile to write to
   * @param rowType of the input records
   */
  PositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
      EncryptedOutputFile outputFile, S rowType);
}
