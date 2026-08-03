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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestFormatModelRegistry {

  @BeforeEach
  void clearRegistry() {
    FormatModelRegistry.models().clear();
  }

  @Test
  void testSuccessfulRegister() {
    FormatModel<?, ?> model = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(model);
    assertThat(FormatModelRegistry.models())
        .containsEntry(Pair.of(FileFormat.PARQUET, Object.class), model);
  }

  /** Tests that registering the same class with the same configuration updates the registration. */
  @Test
  void testRegistrationForDifferentType() {
    FormatModel<?, ?> model1 = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModel<?, ?> model2 = new DummyParquetFormatModel(Long.class, Object.class);
    FormatModelRegistry.register(model1);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model1.type())))
        .isSameAs(model1);

    // Registering a new model with the different format will succeed
    FormatModelRegistry.register(model2);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model1.type())))
        .isSameAs(model1);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model2.type())))
        .isSameAs(model2);
  }

  /**
   * Tests that registering different classes, or different schema type for the same file format and
   * type is failing.
   */
  @Test
  void testFailingReRegistrations() {
    FormatModel<?, ?> model = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(model);
    assertThat(FormatModelRegistry.models())
        .containsEntry(Pair.of(FileFormat.PARQUET, Object.class), model);

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
  }

  @Test
  void registerToleratesMissingClass() {
    assertThatNoException().isThrownBy(() -> register("org.apache.iceberg.formats.DoesNotExist"));
    assertThat(FormatModelRegistry.models()).isEmpty();
  }

  @Test
  void registerToleratesNoClassDefFoundErrorOnInvoke() {
    assertThatNoException().isThrownBy(() -> register(ThrowsOnInvoke.class.getName()));
    assertThat(FormatModelRegistry.models()).isEmpty();
  }

  @Test
  void registerToleratesExceptionInInitializerErrorOnInvoke() {
    assertThatNoException().isThrownBy(() -> register(ThrowsInitErrorOnInvoke.class.getName()));
    assertThat(FormatModelRegistry.models()).isEmpty();
  }

  private static void register(String className) throws ReflectiveOperationException {
    Method register = FormatModelRegistry.class.getDeclaredMethod("register", String.class);
    register.setAccessible(true);
    register.invoke(null, className);
  }

  public static class ThrowsOnInvoke {
    public static void register() {
      throw new NoClassDefFoundError("some/missing/TransitiveDependency");
    }
  }

  public static class ThrowsInitErrorOnInvoke {
    public static void register() {
      throw new ExceptionInInitializerError(new RuntimeException("lazy init failed"));
    }
  }

  @Test
  void equalityDeleteWriterRejectsMissingEqualityFieldId() {
    FormatModelRegistry.register(new DummyParquetFormatModel(Object.class, Object.class));
    EncryptedOutputFile outputFile =
        EncryptedFiles.encryptedOutput(new InMemoryOutputFile(), EncryptionKeyMetadata.EMPTY);
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    assertThatThrownBy(
            () ->
                FormatModelRegistry.equalityDeleteWriteBuilder(
                        FileFormat.PARQUET, Object.class, outputFile)
                    .schema(schema)
                    .spec(PartitionSpec.unpartitioned())
                    .equalityFieldIds(99)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid equality delete field ID: 99");
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
    @SuppressWarnings("unchecked")
    public Class<Object> type() {
      return (Class<Object>) type;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Object> schemaType() {
      return (Class<Object>) schemaType;
    }

    @Override
    public ModelWriteBuilder<Object, Object> writeBuilder(EncryptedOutputFile outputFile) {
      return new DummyModelWriteBuilder();
    }

    @Override
    public ReadBuilder<Object, Object> readBuilder(InputFile inputFile) {
      return null;
    }
  }

  private static class DummyModelWriteBuilder implements ModelWriteBuilder<Object, Object> {
    @Override
    public ModelWriteBuilder<Object, Object> schema(Schema schema) {
      return this;
    }

    @Override
    public ModelWriteBuilder<Object, Object> engineSchema(Object schema) {
      return this;
    }

    @Override
    public ModelWriteBuilder<Object, Object> set(String property, String value) {
      return this;
    }

    @Override
    public ModelWriteBuilder<Object, Object> meta(String property, String value) {
      return this;
    }

    @Override
    public ModelWriteBuilder<Object, Object> content(FileContent content) {
      return this;
    }

    @Override
    public ModelWriteBuilder<Object, Object> metricsConfig(MetricsConfig metricsConfig) {
      return this;
    }

    @Override
    public ModelWriteBuilder<Object, Object> overwrite() {
      return this;
    }

    @Override
    public ModelWriteBuilder<Object, Object> withFileEncryptionKey(ByteBuffer encryptionKey) {
      return this;
    }

    @Override
    public ModelWriteBuilder<Object, Object> withAADPrefix(ByteBuffer aadPrefix) {
      return this;
    }

    @Override
    public FileAppender<Object> build() {
      return new NoOpFileAppender();
    }
  }

  private static class NoOpFileAppender implements FileAppender<Object> {
    @Override
    public void add(Object datum) {}

    @Override
    public Metrics metrics() {
      return null;
    }

    @Override
    public long length() {
      return 0;
    }

    @Override
    public void close() {}
  }
}
