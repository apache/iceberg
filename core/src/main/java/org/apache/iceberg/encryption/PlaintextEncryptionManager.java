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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaintextEncryptionManager implements EncryptionManager {
  private static final EncryptionManager INSTANCE = new PlaintextEncryptionManager();
  private static final Logger LOG = LoggerFactory.getLogger(PlaintextEncryptionManager.class);

  /** @deprecated will be removed in 1.6.0. use {@link #instance()} instead. */
  @Deprecated
  public PlaintextEncryptionManager() {}

  public static EncryptionManager instance() {
    return INSTANCE;
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    if (encrypted.keyMetadata().buffer() != null) {
      LOG.warn("File encryption key metadata is present, but no encryption has been configured.");
    }
    return encrypted.encryptedInputFile();
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    return EncryptedFiles.encryptedOutput(rawOutput, EncryptionKeyMetadata.empty());
  }
}
