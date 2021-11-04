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

import org.apache.iceberg.io.OutputFile;

class NativeEncryptedOutputFile implements EncryptedOutputFile {

  private final OutputFile rawOutput;
  private final NativeFileCryptoParameters nativeEncryptionParameters;
  private final EncryptionKeyMetadata keyMetadata;

  NativeEncryptedOutputFile(OutputFile rawOutput, EncryptionKeyMetadata keyMetadata,
                            NativeFileCryptoParameters nativeEncryptionParameters) {
    this.rawOutput = rawOutput;
    this.nativeEncryptionParameters = nativeEncryptionParameters;
    this.keyMetadata = keyMetadata;
  }

  @Override
  public OutputFile encryptingOutputFile() {
    return rawOutput;
  }

  @Override
  public EncryptionKeyMetadata keyMetadata() {
    return keyMetadata;
  }

  @Override
  public NativeFileCryptoParameters nativeEncryptionParameters() {
    return nativeEncryptionParameters;
  }

  @Override
  public boolean useNativeEncryption() {
    return true;
  }
}
