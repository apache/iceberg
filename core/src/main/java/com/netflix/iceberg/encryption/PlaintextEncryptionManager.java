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
import com.netflix.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class PlaintextEncryptionManager implements EncryptionManager {
  private static final Logger LOG = LoggerFactory.getLogger(PlaintextEncryptionManager.class);

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    if (encrypted.keyMetadata().buffer() != null) {
      LOG.warn("File encryption key metadata is present, but currently using PlaintextEncryptionManager.");
    }
    return encrypted.encryptedInputFile();
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile rawOutput) {
    return EncryptedFiles.encryptedOutput(rawOutput, (ByteBuffer) null);
  }
}
