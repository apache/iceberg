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
package org.apache.iceberg.io.canonicaluri;

import org.apache.iceberg.encryption.NativeEncryptionInputFile;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

class CanonicalInputFile implements InputFile {
  private final String canonicalLocation;
  private final InputFile delegate;
  private final PathAliasMetrics metrics;

  static InputFile wrap(String canonicalLocation, InputFile delegate, PathAliasMetrics metrics) {
    if (delegate instanceof NativeEncryptionInputFile) {
      return new CanonicalNativeInputFile(
          canonicalLocation, (NativeEncryptionInputFile) delegate, metrics);
    }

    return new CanonicalInputFile(canonicalLocation, delegate, metrics);
  }

  CanonicalInputFile(String canonicalLocation, InputFile delegate, PathAliasMetrics metrics) {
    this.canonicalLocation = canonicalLocation;
    this.delegate = delegate;
    this.metrics = metrics;
  }

  @Override
  public long getLength() {
    return call(delegate::getLength);
  }

  @Override
  public SeekableInputStream newStream() {
    return CanonicalSeekableInputStream.wrap(call(delegate::newStream));
  }

  @Override
  public String location() {
    return canonicalLocation;
  }

  @Override
  public boolean exists() {
    return call(delegate::exists);
  }

  private <T> T call(Call<T> call) {
    try {
      return call.invoke();
    } catch (RuntimeException e) {
      metrics.delegateFailed();
      throw e;
    }
  }

  private interface Call<T> {
    T invoke();
  }

  private static class CanonicalNativeInputFile extends CanonicalInputFile
      implements NativeEncryptionInputFile {
    private final String canonicalLocation;
    private final NativeEncryptionInputFile nativeDelegate;
    private final PathAliasMetrics metrics;

    private CanonicalNativeInputFile(
        String canonicalLocation, NativeEncryptionInputFile delegate, PathAliasMetrics metrics) {
      super(canonicalLocation, delegate, metrics);
      this.canonicalLocation = canonicalLocation;
      this.nativeDelegate = delegate;
      this.metrics = metrics;
    }

    @Override
    public InputFile encryptedInputFile() {
      return wrap(canonicalLocation, nativeDelegate.encryptedInputFile(), metrics);
    }

    @Override
    public NativeEncryptionKeyMetadata keyMetadata() {
      return nativeDelegate.keyMetadata();
    }
  }
}
