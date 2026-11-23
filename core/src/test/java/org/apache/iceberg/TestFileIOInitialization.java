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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestFileIOInitialization {
  private static final String IMPL = FailingFileIO.class.getName();

  @BeforeEach
  void resetFileIO() {
    FailingFileIO.reset();
  }

  @ParameterizedTest
  @ValueSource(strings = {"configuration", "credentials", "initialize"})
  void closesFileIOWhenSetupFails(String stage) {
    RuntimeException failure = new IllegalStateException(stage);
    FailingFileIO.failAt(stage, failure);

    assertThatThrownBy(() -> CatalogUtil.loadFileIO(IMPL, Map.of(), new Object(), List.of()))
        .isSameAs(failure);

    assertThat(FailingFileIO.instance.closeCount).isEqualTo(1);
  }

  @Test
  void suppressesCloseFailureWhenSetupFails() {
    RuntimeException failure = new IllegalStateException("initialize");
    RuntimeException closeFailure = new IllegalStateException("close");
    FailingFileIO.failAt("initialize", failure);
    FailingFileIO.closeFailure = closeFailure;

    assertThatThrownBy(() -> CatalogUtil.loadFileIO(IMPL, Map.of(), new Object(), List.of()))
        .isSameAs(failure)
        .hasSuppressedException(closeFailure);
  }

  @Test
  void returnsOpenFileIOWhenSetupSucceeds() {
    FileIO io = CatalogUtil.loadFileIO(IMPL, Map.of(), new Object(), List.of());

    assertThat(io).isSameAs(FailingFileIO.instance);
    assertThat(FailingFileIO.instance.closeCount).isZero();
  }

  public static class FailingFileIO
      implements FileIO, Configurable<Object>, SupportsStorageCredentials {
    private static FailingFileIO instance;
    private static String failStage;
    private static RuntimeException runtimeFailure;
    private static RuntimeException closeFailure;

    private int closeCount = 0;

    public FailingFileIO() {
      instance = this;
    }

    static void reset() {
      instance = null;
      failStage = null;
      runtimeFailure = null;
      closeFailure = null;
    }

    static void failAt(String stage, RuntimeException failure) {
      failStage = stage;
      runtimeFailure = failure;
    }

    private static void maybeFail(String stage) {
      if (stage.equals(failStage)) {
        throw runtimeFailure;
      }
    }

    @Override
    public void setConf(Object conf) {
      maybeFail("configuration");
    }

    @Override
    public void setCredentials(List<StorageCredential> credentials) {
      maybeFail("credentials");
    }

    @Override
    public List<StorageCredential> credentials() {
      return List.of();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      maybeFail("initialize");
    }

    @Override
    public void close() {
      closeCount++;
      if (closeFailure != null) {
        throw closeFailure;
      }
    }

    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }
}
