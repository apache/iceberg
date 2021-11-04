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
import org.apache.iceberg.io.WrappedInputStream;

public class NativeEncryptedInputFile implements InputFile {

  private final InputFile rawInput;
  private final NativeFileCryptoParameters nativeDecryptionParameteres;

  NativeEncryptedInputFile(InputFile rawInput, NativeFileCryptoParameters nativeDecryptionParameteres) {
    this.rawInput = rawInput;
    this.nativeDecryptionParameteres = nativeDecryptionParameteres;
  }

  public NativeFileCryptoParameters nativeDecryptionParameters() {
    return nativeDecryptionParameteres;
  }

  @Override
  public long getLength() {
    return rawInput.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    // TODO remove this comment after review
    // This class is not HadoopInputFile, while its rawInput can be; and rawInput's stream can be FSDataInputStream.
    // Returning rawInput.newStream() here leads to closed stream exceptions, due to Hadoop handling in ParquetIO class.
    // Therefore, using stream wrap.
    return new WrappedInputStream(rawInput.newStream());
  }

  @Override
  public String location() {
    return rawInput.location();
  }

  @Override
  public boolean exists() {
    return rawInput.exists();
  }
}
