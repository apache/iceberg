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

package com.netflix.iceberg.encryption;

import com.netflix.iceberg.io.InputFile;

import java.nio.ByteBuffer;

/**
 * Thin wrapper around an {@link InputFile} instance that is encrypted.
 * <p>
 * The {@link EncryptionManager} takes instances of these and uses the attached
 * {@link #keyMetadata()} to find an encryption key and decrypt the enclosed
 * {@link #encryptedInputFile()}.
 */
public interface EncryptedInputFile {

  /**
   * The {@link InputFile} that is reading raw encrypted bytes from the underlying file system.
   */
  InputFile encryptedInputFile();

  /**
   * Metadata pointing to some encryption key that would be used to decrypt the input file provided
   * by {@link #encryptedInputFile()}.
   */
  EncryptionKeyMetadata keyMetadata();
}
