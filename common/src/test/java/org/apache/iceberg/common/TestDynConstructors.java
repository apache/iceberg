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

public class TestDynConstructors {
  @Test
  public void testImplNewInstance() throws Exception {
    DynConstructors.Ctor<MyClass> ctor =
        DynConstructors.builder().impl(MyClass.class).buildChecked();
    assertThat(ctor.newInstance()).isInstanceOf(MyClass.class);
  }

  @Test
  public void testInterfaceImplNewInstance() throws Exception {
    DynConstructors.Ctor<MyInterface> ctor =
        DynConstructors.builder(MyInterface.class)
            .impl("org.apache.iceberg.common.TestDynConstructors$MyClass")
            .buildChecked();
    assertThat(ctor.newInstance()).isInstanceOf(MyClass.class);
  }

  @Test
  public void testInterfaceWrongImplString() throws Exception {
    DynConstructors.Ctor<MyInterface> ctor =
        DynConstructors.builder(MyInterface.class)
            // TODO this should throw, since the MyUnrelatedClass does not implement MyInterface
            .impl("org.apache.iceberg.common.TestDynConstructors$MyUnrelatedClass")
            .buildChecked();
    assertThatThrownBy(ctor::newInstance)
        .isInstanceOf(ClassCastException.class)
        .hasMessage(
            "org.apache.iceberg.common.TestDynConstructors$MyUnrelatedClass cannot be cast to org.apache.iceberg.common.TestDynConstructors$MyInterface");
  }

  @Test
  public void testInterfaceWrongImplClass() throws Exception {
    DynConstructors.Ctor<MyInterface> ctor =
        DynConstructors.builder(MyInterface.class)
            // TODO this should throw or not compile at all, since the MyUnrelatedClass does not
            // implement MyInterface
            .impl(MyUnrelatedClass.class)
            .buildChecked();
    assertThatThrownBy(ctor::newInstance)
        .isInstanceOf(ClassCastException.class)
        .hasMessage(
            "org.apache.iceberg.common.TestDynConstructors$MyUnrelatedClass cannot be cast to org.apache.iceberg.common.TestDynConstructors$MyInterface");
  }

  public interface MyInterface {}

  public static class MyClass implements MyInterface {}

  public static class MyUnrelatedClass {}
}
