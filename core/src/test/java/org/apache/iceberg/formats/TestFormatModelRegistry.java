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
package org.apache.iceberg.formats;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.Test;

class TestFormatModelRegistry {
  /** Tests that registering the same class with the same configuration is successful. */
  @Test
  void testSuccessfulReRegister() {
    FormatModel<?, ?> model = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(model);
    assertThat(FormatModelRegistry.models())
        .containsEntry(Pair.of(FileFormat.PARQUET, Object.class), model);
    FormatModelRegistry.register(model);

    // It is fine to register the same classes with the same configuration multiple times
    assertThat(FormatModelRegistry.models())
        .containsEntry(Pair.of(FileFormat.PARQUET, Object.class), model);
  }

  /** Tests that registering the same class with the same configuration updates the registration. */
  @Test
  void testUpdatedRegistration() {
    FormatModel<?, ?> model1 = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModel<?, ?> model2 = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(model1);
    assertThat(FormatModelRegistry.models().get(Pair.of(model1.format(), model1.type())))
        .isSameAs(model1);

    // Registering a new model with the same format and schema type should replace the old one
    FormatModelRegistry.register(model2);
    assertThat(FormatModelRegistry.models().get(Pair.of(model1.format(), model1.type())))
        .isNotSameAs(model1);
    assertThat(FormatModelRegistry.models().get(Pair.of(model1.format(), model1.type())))
        .isSameAs(model2);
  }

  /**
   * Tests that registering different classes, or different schema type for the same file format and
   * type is failing.
   */
  @Test
  void testFailingReRegistrations() {
    FormatModel<?, ?> existing = new OtherDummyParquetFormatModel();
    FormatModel<?, ?> model = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(existing);
    FormatModelRegistry.register(model);
    assertThat(FormatModelRegistry.models().get(Pair.of(model.format(), model.type())))
        .isSameAs(model);

    // Registering a new model with different schema type should fail
    assertThatThrownBy(
            () ->
                FormatModelRegistry.register(
                    new DummyParquetFormatModel(Object.class, String.class)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register class");

    // Registering a new model with null schema type should fail
    assertThatThrownBy(
            () -> FormatModelRegistry.register(new DummyParquetFormatModel(Object.class, null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register class");

    // Registering a new model with different class should fail
    assertThatThrownBy(
            () ->
                FormatModelRegistry.register(
                    new DummyParquetFormatModel(existing.type(), existing.schemaType())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register class");
  }

  private static class DummyParquetFormatModel implements FormatModel<Object, Object> {
    private final Class<?> type;
    private final Class<?> schemaType;

    private DummyParquetFormatModel(Class<?> type, Class<?> schemaType) {
      this.type = type;
      this.schemaType = schemaType;
    }

    @Override
    public FileFormat format() {
      return FileFormat.PARQUET;
    }

    @Override
    public Class<Object> type() {
      return (Class<Object>) type;
    }

    @Override
    public Class<Object> schemaType() {
      return (Class<Object>) schemaType;
    }

    @Override
    public WriteBuilder writeBuilder(OutputFile outputFile) {
      return null;
    }

    @Override
    public ReadBuilder readBuilder(InputFile inputFile) {
      return null;
    }
  }

  private static class OtherDummyParquetFormatModel extends DummyParquetFormatModel {
    private OtherDummyParquetFormatModel() {
      super(Long.class, null);
    }
  }
}
