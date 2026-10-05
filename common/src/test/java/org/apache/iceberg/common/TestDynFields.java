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
package org.apache.iceberg.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TestDynFields {
  static class FieldHolder {
    public String value = "hello";

    @SuppressWarnings("unused")
    private String hidden = "secret";
  }

  @Test
  void implWithNoClassDefFoundError() throws NoSuchFieldException {
    ClassLoader errorLoader =
        new ClassLoader(Thread.currentThread().getContextClassLoader()) {
          @Override
          public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if ("org.apache.iceberg.MissingDependencyClass".equals(name)) {
              throw new NoClassDefFoundError("some/TransitiveDependency");
            }

            return super.loadClass(name, resolve);
          }
        };

    assertThatThrownBy(
            () ->
                DynFields.builder()
                    .loader(errorLoader)
                    .impl("org.apache.iceberg.MissingDependencyClass", "value")
                    .buildChecked())
        .isInstanceOf(NoSuchFieldException.class)
        .hasMessage(
            "Cannot find field from candidates: org.apache.iceberg.MissingDependencyClass.value");

    assertThat(
            DynFields.builder()
                .loader(errorLoader)
                .impl("org.apache.iceberg.MissingDependencyClass", "value")
                .impl(FieldHolder.class, "value")
                .<String>buildChecked()
                .get(new FieldHolder()))
        .isEqualTo("hello");

    assertThat(
            DynFields.builder()
                .loader(errorLoader)
                .hiddenImpl("org.apache.iceberg.MissingDependencyClass", "hidden")
                .hiddenImpl(FieldHolder.class, "hidden")
                .<String>buildChecked()
                .get(new FieldHolder()))
        .isEqualTo("secret");
  }

  @Test
  void implWithExceptionInInitializerError() {
    ClassLoader errorLoader =
        new ClassLoader(Thread.currentThread().getContextClassLoader()) {
          @Override
          public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            throw new ExceptionInInitializerError("static initializer failed");
          }
        };

    assertThatThrownBy(
            () ->
                DynFields.builder()
                    .loader(errorLoader)
                    .impl("org.apache.iceberg.FailingInitClass", "value")
                    .buildChecked())
        .isInstanceOf(ExceptionInInitializerError.class)
        .hasMessage("static initializer failed");
  }
}
