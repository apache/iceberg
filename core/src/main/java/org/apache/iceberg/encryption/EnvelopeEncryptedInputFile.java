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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * An input file that decrypts data based on the given envelope encryption metadata.
 */
public class EnvelopeEncryptedInputFile implements InputFile {

  private final InputFile encryptedInput;
  private final EnvelopeMetadata metadata;
  private final boolean pushdownDecryption;

  /**
   * Constructor
   *
   * @param encryptedInput encrypted input file
   * @param metadata       metadata of type {@link BaseEncryptionKeyMetadata} that only wraps the bytes
   */
  public EnvelopeEncryptedInputFile(InputFile encryptedInput, EnvelopeMetadata metadata, boolean pushdown) {
    this.encryptedInput = encryptedInput;
    this.metadata = metadata;
    this.pushdownDecryption = pushdown;
  }

  @Override
  public long getLength() {
    return encryptedInput.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    return new EnvelopeDecryptingInputStream(encryptedInput.newStream(), metadata);
  }

  @Override
  public String location() {
    return encryptedInput.location();
  }

  @Override
  public boolean exists() {
    return encryptedInput.exists();
  }

  public boolean useNativeDecryption() {
    return pushdownDecryption;
  }

  public NativeFileDecryptParameters nativeDecryptionParameters() {
    Map<String, ByteBuffer> columnKeys = null;
    if (!metadata.columnMetadata().isEmpty()) {
      columnKeys = new HashMap<>();
      for (EnvelopeMetadata column : metadata.columnMetadata()) {
        columnKeys.put(column.originalColumnName(), column.dek());
      }
    }

    return NativeFileDecryptParameters
            .create(metadata.dek())
            .columnKeys(columnKeys)
            .build();
  }
}
