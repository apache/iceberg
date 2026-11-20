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

import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

class CanonicalOutputFile implements OutputFile {
  private final String canonicalLocation;
  private final OutputFile delegate;
  private final PathAliasMetrics metrics;

  static OutputFile wrap(String canonicalLocation, OutputFile delegate, PathAliasMetrics metrics) {
    if (delegate instanceof NativeEncryptionOutputFile) {
      return new CanonicalNativeOutputFile(
          canonicalLocation, (NativeEncryptionOutputFile) delegate, metrics);
    }

    return new CanonicalOutputFile(canonicalLocation, delegate, metrics);
  }

  CanonicalOutputFile(String canonicalLocation, OutputFile delegate, PathAliasMetrics metrics) {
    this.canonicalLocation = canonicalLocation;
    this.delegate = delegate;
    this.metrics = metrics;
  }

  @Override
  public PositionOutputStream create() {
    return new CanonicalPositionOutputStream(call(delegate::create));
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return new CanonicalPositionOutputStream(call(delegate::createOrOverwrite));
  }

  @Override
  public String location() {
    return canonicalLocation;
  }

  @Override
  public InputFile toInputFile() {
    return CanonicalInputFile.wrap(canonicalLocation, call(delegate::toInputFile), metrics);
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

  private static class CanonicalNativeOutputFile extends CanonicalOutputFile
      implements NativeEncryptionOutputFile {
    private final String canonicalLocation;
    private final NativeEncryptionOutputFile nativeDelegate;
    private final PathAliasMetrics metrics;

    private CanonicalNativeOutputFile(
        String canonicalLocation, NativeEncryptionOutputFile delegate, PathAliasMetrics metrics) {
      super(canonicalLocation, delegate, metrics);
      this.canonicalLocation = canonicalLocation;
      this.nativeDelegate = delegate;
      this.metrics = metrics;
    }

    @Override
    public OutputFile encryptingOutputFile() {
      return wrap(canonicalLocation, nativeDelegate.encryptingOutputFile(), metrics);
    }

    @Override
    public OutputFile plainOutputFile() {
      return wrap(canonicalLocation, nativeDelegate.plainOutputFile(), metrics);
    }

    @Override
    public NativeEncryptionKeyMetadata keyMetadata() {
      return nativeDelegate.keyMetadata();
    }
  }
}
