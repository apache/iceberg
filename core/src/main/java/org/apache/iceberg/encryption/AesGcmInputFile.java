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
  private long plaintextLength;

  public AesGcmInputFile(InputFile sourceFile, byte[] dataKey, byte[] fileAADPrefix) {
    this.sourceFile = sourceFile;
    this.dataKey = dataKey;
    this.fileAADPrefix = fileAADPrefix;
    this.plaintextLength = -1;
  }

  @Override
  public long getLength() {
    if (plaintextLength == -1) {
      // Presumes all streams use hard-coded plaintext block size.
      plaintextLength = AesGcmInputStream.calculatePlaintextLength(sourceFile.getLength());
    }

    return plaintextLength;
  }

  @Override
  public SeekableInputStream newStream() {
    long ciphertextLength = sourceFile.getLength();
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
