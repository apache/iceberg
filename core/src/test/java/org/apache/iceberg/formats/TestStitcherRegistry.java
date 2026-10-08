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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Method;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestStitcherRegistry {
  @AfterEach
  void unregisterStitcherBuilders() {
    StitcherRegistry.stitcherBuilders().remove(Row.class);
  }

  @Test
  void findsTheStitcherBuilderRegisteredForTheRowType() {
    StitcherBuilder<Row> builder = stitcherBuilder();
    StitcherRegistry.register(builder);

    assertThat(StitcherRegistry.stitcherBuilder(Row.class)).isSameAs(builder);
  }

  @Test
  void rejectsRegisteringTwoStitcherBuildersForTheSameRowType() {
    StitcherRegistry.register(stitcherBuilder());

    assertThatThrownBy(() -> StitcherRegistry.register(stitcherBuilder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is registered for type=" + Row.class);
  }

  @Test
  void rejectsLookingUpAnUnregisteredRowType() {
    assertThatThrownBy(() -> StitcherRegistry.stitcherBuilder(Row.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot read a data file with column files: no stitcher is registered for type %s",
            Row.class);
  }

  @SuppressWarnings("unchecked")
  private StitcherBuilder<Row> stitcherBuilder() {
    StitcherBuilder<Row> builder = mock(StitcherBuilder.class);
    doReturn(Row.class).when(builder).type();
    return builder;
  }

  @Test
  void registerToleratesMissingClass() {
    assertThatNoException().isThrownBy(() -> register("org.apache.iceberg.formats.DoesNotExist"));
  }

  @Test
  void registerToleratesModuleWithoutStitchers() {
    assertThatNoException().isThrownBy(() -> register(HasNoRegisterMethod.class.getName()));
  }

  @Test
  void registerToleratesNoClassDefFoundErrorOnInvoke() {
    assertThatNoException().isThrownBy(() -> register(ThrowsOnInvoke.class.getName()));
  }

  @Test
  void registerToleratesExceptionInInitializerErrorOnInvoke() {
    assertThatNoException().isThrownBy(() -> register(ThrowsInitErrorOnInvoke.class.getName()));
  }

  private static void register(String className) throws ReflectiveOperationException {
    Method register = StitcherRegistry.class.getDeclaredMethod("register", String.class);
    register.setAccessible(true);
    register.invoke(null, className);
  }

  public static class HasNoRegisterMethod {}

  public static class ThrowsOnInvoke {
    public static void register() {
      throw new NoClassDefFoundError("some/missing/TransitiveDependency");
    }
  }

  public static class ThrowsInitErrorOnInvoke {
    public static void register() {
      throw new ExceptionInInitializerError("static initializer failed");
    }
  }

  /** Row type used by the stitcher builders of this test. */
  private static class Row {}
}
