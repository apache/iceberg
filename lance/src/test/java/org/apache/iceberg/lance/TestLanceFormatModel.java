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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the LanceFormatModel.
 *
 * <p>Note: Full round-trip tests require the Lance JNI native library to be available. These tests
 * validate the model configuration and builder wiring without requiring native code.
 */
class TestLanceFormatModel {

  @TempDir Path tempDir;

  @Test
  void testFormatReturnsLance() {
    LanceFormatModel<Record, Void> model =
        LanceFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, arrowSchema, engineSchema) ->
                GenericLanceWriter.create(icebergSchema, arrowSchema),
            (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
                GenericLanceReader.buildReader(icebergSchema, arrowSchema, idToConstant));

    assertThat(model.format()).isEqualTo(FileFormat.LANCE);
  }

  @Test
  void testTypeAndSchemaType() {
    LanceFormatModel<Record, Void> model =
        LanceFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, arrowSchema, engineSchema) ->
                GenericLanceWriter.create(icebergSchema, arrowSchema),
            (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
                GenericLanceReader.buildReader(icebergSchema, arrowSchema, idToConstant));

    assertThat(model.type()).isEqualTo(Record.class);
    assertThat(model.schemaType()).isEqualTo(Void.class);
  }

  @Test
  void testPositionDeleteModel() {
    LanceFormatModel<?, Void> model = LanceFormatModel.forPositionDeletes();

    assertThat(model.format()).isEqualTo(FileFormat.LANCE);
  }

  @Test
  void testWriteBuilderEncryptionThrows() {
    LanceFormatModel<Record, Void> model =
        LanceFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, arrowSchema, engineSchema) ->
                GenericLanceWriter.create(icebergSchema, arrowSchema),
            (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
                GenericLanceReader.buildReader(icebergSchema, arrowSchema, idToConstant));

    EncryptedOutputFile encryptedOutput =
        EncryptedFiles.plainAsEncryptedOutput(
            Files.localOutput(tempDir.resolve("enc-test.lance").toFile()));

    ModelWriteBuilder<Record, Void> builder = model.writeBuilder(encryptedOutput);

    assertThatThrownBy(() -> builder.withFileEncryptionKey(ByteBuffer.allocate(16)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Lance does not support file encryption");

    assertThatThrownBy(() -> builder.withAADPrefix(ByteBuffer.allocate(16)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Lance does not support file encryption");
  }
}
