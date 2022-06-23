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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

public class AesGcmInputFile implements InputFile {
  private final InputFile sourceFile;
  private final byte[] dataKey;
  private long plaintextLength;

  public AesGcmInputFile(InputFile sourceFile, byte[] dataKey) {
    this.sourceFile = sourceFile;
    this.dataKey = dataKey;
    this.plaintextLength = -1;
  }

  @Override
  public long getLength() {
    if (plaintextLength == -1) {
      try {
        this.newStream().close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return plaintextLength;
  }

  @Override
  public SeekableInputStream newStream() {
    AesGcmInputStream result;
    try {
      result = new AesGcmInputStream(sourceFile.newStream(), sourceFile.getLength(), dataKey, null);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    plaintextLength = result.plaintextStreamSize();
    return result;
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
