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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the hot-path classes used as Flink stream element types are valid POJO types. Flink
 * POJO serialization requires a public no-arg constructor and public setters for all fields.
 */
class TestFlinkPojoTypes {

  @Test
  void testSerializedEqualityValues() {
    assertThat(TypeInformation.of(SerializedEqualityValues.class)).isInstanceOf(PojoTypeInfo.class);
  }

  @Test
  void testDVPosition() {
    assertThat(TypeInformation.of(DVPosition.class)).isInstanceOf(PojoTypeInfo.class);
  }

  @Test
  void testIndexCommand() {
    assertThat(TypeInformation.of(IndexCommand.class)).isInstanceOf(PojoTypeInfo.class);
  }
}
