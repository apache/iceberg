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
package org.apache.iceberg.encryption;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/** An {@link EncryptedOutputFile} that can be used for format-native encryption. */
public interface NativeEncryptionOutputFile extends EncryptedOutputFile, OutputFile {
  @Override
  NativeEncryptionKeyMetadata keyMetadata();

  /** An {@link OutputFile} instance for the underlying (plaintext) output stream. */
  OutputFile plainOutputFile();

  @Override
  default PositionOutputStream create() {
    return encryptingOutputFile().create();
  }

  @Override
  default PositionOutputStream createOrOverwrite() {
    return encryptingOutputFile().createOrOverwrite();
  }

  @Override
  default String location() {
    return encryptingOutputFile().location();
  }

  @Override
  default InputFile toInputFile() {
    return encryptingOutputFile().toInputFile();
  }
}
