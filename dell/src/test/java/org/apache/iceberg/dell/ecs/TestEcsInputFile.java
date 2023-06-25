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
package org.apache.iceberg.dell.ecs;

import static org.assertj.core.api.Assertions.assertThat;

import com.emc.object.s3.request.PutObjectRequest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.common.testutils.PerClassCallbackWrapper;
import org.apache.iceberg.dell.mock.ecs.EcsS3MockExtension;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestEcsInputFile {

  @RegisterExtension
  static PerClassCallbackWrapper<EcsS3MockExtension> extensionWrapper =
      new PerClassCallbackWrapper<>(EcsS3MockExtension.create());

  @Test
  void testAbsentFile() {
    String objectName = extensionWrapper.getExtension().randomObjectName();
    EcsInputFile inputFile =
        EcsInputFile.fromLocation(
            new EcsURI(extensionWrapper.getExtension().bucket(), objectName).toString(),
            extensionWrapper.getExtension().client());
    assertThat(inputFile.exists()).as("File is absent").isFalse();
  }

  @Test
  void testFileRead() throws IOException {
    String objectName = extensionWrapper.getExtension().randomObjectName();
    EcsInputFile inputFile =
        EcsInputFile.fromLocation(
            new EcsURI(extensionWrapper.getExtension().bucket(), objectName).toString(),
            extensionWrapper.getExtension().client());

    extensionWrapper
        .getExtension()
        .client()
        .putObject(
            new PutObjectRequest(
                extensionWrapper.getExtension().bucket(), objectName, "0123456789".getBytes()));

    assertThat(inputFile.exists()).as("File should exists").isTrue();
    assertThat(inputFile.getLength()).as("File length should be 10").isEqualTo(10);
    try (InputStream inputStream = inputFile.newStream()) {
      assertThat(new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8))
          .as("The file content should be 0123456789")
          .isEqualTo("0123456789");
    }
  }
}
