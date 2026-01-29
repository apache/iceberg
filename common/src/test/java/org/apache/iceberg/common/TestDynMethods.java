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

import org.junit.jupiter.api.Test;

public class TestDynMethods {
  @Test
  public void testHiddenImpl() {
    String value = "Hello";
    var method =
        DynMethods.builder("hiddenUpper")
            .hiddenImpl(ExperimentalDynMethods.class, String.class)
            .build();
    var experimental = new ExperimentalDynMethods();
    assertThat((String) method.invoke(experimental, value)).isEqualTo(value.toUpperCase());
  }

  private static class ExperimentalDynMethods {
    private String hiddenUpper(String input) {
      return input.toUpperCase();
    }
  }
}
