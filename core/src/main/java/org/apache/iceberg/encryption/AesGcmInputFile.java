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
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class AesGcmInputFile implements InputFile {
  private final InputFile sourceFile;
  private final byte[] dataKey;
  private final byte[] fileAADPrefix;
  private Long encryptedLength;
  private Long plaintextLength;

  public AesGcmInputFile(InputFile sourceFile, byte[] dataKey, byte[] fileAADPrefix) {
    this(sourceFile, dataKey, fileAADPrefix, null);
  }

  public AesGcmInputFile(InputFile sourceFile, byte[] dataKey, byte[] fileAADPrefix, Long length) {
    this.sourceFile = sourceFile;
    this.dataKey = dataKey;
    this.fileAADPrefix = fileAADPrefix;
    this.encryptedLength = length;
    this.plaintextLength = null;
  }

  private long encryptedLength() {
    if (encryptedLength == null) {
      this.encryptedLength = sourceFile.getLength();
    }

    return encryptedLength;
  }

  @Override
  public long getLength() {
    if (plaintextLength == null) {
      // Presumes all streams use hard-coded plaintext block size.
      plaintextLength = AesGcmInputStream.calculatePlaintextLength(encryptedLength());
    }

    return plaintextLength;
  }

  @Override
  public SeekableInputStream newStream() {
    long ciphertextLength = encryptedLength();
    Preconditions.checkState(
        ciphertextLength >= Ciphers.MIN_STREAM_LENGTH,
        "Invalid encrypted stream: %d is shorter than the minimum possible stream length",
        ciphertextLength);
    return new AesGcmInputStream(sourceFile.newStream(), ciphertextLength, dataKey, fileAADPrefix);
  }

  @Override
  public String location() {
    return sourceFile.location();
  }

  @Override
  public boolean exists() {
    return sourceFile.exists();
  }
}
