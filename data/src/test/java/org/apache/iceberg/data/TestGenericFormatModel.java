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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.Test;

/**
 * File Format API TCK binding for the generic {@link Record} object model.
 *
 * <p>Unlike the Spark and Flink bindings, the generic engine represents every Iceberg type
 * directly, so it exercises the full generator matrix (including geospatial types) against the
 * Avro, ORC and Parquet object models. Format-level gaps (for example ORC's lack of geospatial
 * support) are handled by {@code MISSING_FEATURES}/{@code supportsGenerator} in the base class.
 */
class TestGenericFormatModel extends BaseFormatModelTests<Record> {

  @Override
  protected Class<Record> engineType() {
    return Record.class;
  }

  @Override
  protected Object engineSchema(Schema schema) {
    // The generic object model derives its schema from the Iceberg schema; no engine-native
    // schema is required.
    return null;
  }

  @Override
  protected Record convertToEngine(Record record, Schema schema) {
    return record;
  }

  @Override
  protected void assertEquals(Schema schema, List<Record> expected, List<Record> actual) {
    DataTestHelpers.assertEquals(schema.asStruct(), expected, actual);
  }

  @Override
  protected Object convertConstantToEngine(Type type, Object value) {
    // Metadata-column constants such as the _partition struct arrive as a StructLike (for example
    // PartitionData) rather than a Record. Materialize them as a Record up front, mirroring how the
    // Flink and Spark bindings convert constants into their engine representation, so reads expose
    // the same type the generic comparison in DataTestHelpers expects.
    if (value instanceof StructLike struct) {
      return structLikeToRecord(struct, type.asStructType());
    }

    // Scalar constants arrive in Iceberg's internal representation (for example dates as epoch-day
    // ints and timestamps as micro longs); convert them to the generic object model representation
    // (LocalDate, LocalDateTime, ...) so reads match the generic comparison in DataTestHelpers.
    return GenericDataUtil.internalToGeneric(type, value);
  }

  @Test
  void registersModelsWhenTheContextClassLoaderCannotSeeIceberg() throws Exception {
    // models are registered once per loaded copy of FormatModelRegistry, so this needs a fresh copy
    try (URLClassLoader isolated =
        new URLClassLoader(TestHelpers.testClasspath(), ClassLoader.getPlatformClassLoader())) {
      AtomicReference<Map<?, ?>> models = new AtomicReference<>();
      AtomicReference<Throwable> failure = new AtomicReference<>();
      Thread thread =
          new Thread(
              () -> {
                try {
                  models.set(registeredModels(isolated));
                } catch (InvocationTargetException e) {
                  failure.set(e.getCause());
                } catch (Throwable e) {
                  failure.set(e);
                }
              });
      thread.setContextClassLoader(null);
      thread.start();
      thread.join();

      assertThat(failure.get()).isNull();
      assertThat(models.get()).isNotEmpty();
    }
  }

  private static Map<?, ?> registeredModels(ClassLoader loader)
      throws ReflectiveOperationException {
    Class<?> registry =
        Class.forName("org.apache.iceberg.formats.FormatModelRegistry", true, loader);
    assertThat(registry).isNotSameAs(FormatModelRegistry.class);

    Method models = registry.getDeclaredMethod("models");
    models.setAccessible(true);
    return (Map<?, ?>) models.invoke(null);
  }
}
