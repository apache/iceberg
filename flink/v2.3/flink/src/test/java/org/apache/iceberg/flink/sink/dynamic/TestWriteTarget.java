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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestWriteTarget {

  private static final WriteTarget TARGET =
      new WriteTarget("table", "main", 1, 0, true, Sets.newHashSet(1));

  @Test
  void testWithoutSchemaIdMatchesTargetsDifferingOnlyInSchemaId() {
    WriteTarget evolved = new WriteTarget("table", "main", 2, 0, true, Sets.newHashSet(1));

    assertThat(TARGET).isNotEqualTo(evolved);
    assertThat(TARGET.withoutSchemaId()).isEqualTo(evolved.withoutSchemaId());
    assertThat(TARGET.withoutSchemaId()).hasSameHashCodeAs(evolved.withoutSchemaId());
    assertThat(TARGET.withoutSchemaId().schemaId()).isNull();
  }

  @Test
  void testWithoutSchemaIdKeepsEveryOtherField() {
    WriteTarget scope = TARGET.withoutSchemaId();

    assertThat(scope.tableName()).isEqualTo(TARGET.tableName());
    assertThat(scope.branch()).isEqualTo(TARGET.branch());
    assertThat(scope.specId()).isEqualTo(TARGET.specId());
    assertThat(scope.upsertMode()).isEqualTo(TARGET.upsertMode());
    assertThat(scope.equalityFields()).isEqualTo(TARGET.equalityFields());

    assertThat(scope)
        .isNotEqualTo(
            new WriteTarget("other", "main", 1, 0, true, Sets.newHashSet(1)).withoutSchemaId())
        .isNotEqualTo(
            new WriteTarget("table", "branch", 1, 0, true, Sets.newHashSet(1)).withoutSchemaId())
        .isNotEqualTo(
            new WriteTarget("table", "main", 1, 1, true, Sets.newHashSet(1)).withoutSchemaId())
        .isNotEqualTo(
            new WriteTarget("table", "main", 1, 0, false, Sets.newHashSet(1)).withoutSchemaId())
        .isNotEqualTo(
            new WriteTarget("table", "main", 1, 0, true, Sets.newHashSet(2)).withoutSchemaId());
  }
}
