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
package org.apache.iceberg.view;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestViewUtil {

  @Test
  public void highestFieldIdWithEmptySchema() {
    assertThat(ViewUtil.highestFieldId(ImmutableList.of())).isEqualTo(0);
  }

  @Test
  public void highestFieldId() {
    assertThat(
            ViewUtil.highestFieldId(
                ImmutableList.of(
                    new Schema(Types.NestedField.required(1, "x", Types.LongType.get())),
                    new Schema(Types.NestedField.required(3, "y", Types.LongType.get())),
                    new Schema(Types.NestedField.required(5, "z", Types.LongType.get())))))
        .isEqualTo(5);
  }

  @Test
  public void assignFreshSchemaIdWhereNewSchemasFieldIdIsSmallerThanHighestFieldId() {
    Schema schemaOne = new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(Types.NestedField.required(2, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(Types.NestedField.required(3, "z", Types.LongType.get()));
    Schema newSchema = new Schema(Types.NestedField.required(1, "a", Types.LongType.get()));

    ViewMetadata metadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .setCurrentVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .schemaId(0)
                    .timestampMillis(System.currentTimeMillis())
                    .defaultNamespace(Namespace.empty())
                    .build(),
                schemaOne)
            .build();

    newSchema = ViewUtil.assignFreshIds(metadata, newSchema);
    assertThat(newSchema.highestFieldId()).isEqualTo(4);
  }

  @Test
  public void assignFreshSchemaIdWhereNewSchemasFieldIdIsGreaterThanHighestFieldId() {
    Schema schemaOne = new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(Types.NestedField.required(2, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(Types.NestedField.required(3, "z", Types.LongType.get()));
    Schema newSchema = new Schema(Types.NestedField.required(12, "a", Types.LongType.get()));

    ViewMetadata metadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .setCurrentVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .schemaId(0)
                    .timestampMillis(System.currentTimeMillis())
                    .defaultNamespace(Namespace.empty())
                    .build(),
                schemaOne)
            .build();

    newSchema = ViewUtil.assignFreshIds(metadata, newSchema);
    assertThat(newSchema.highestFieldId()).isEqualTo(4);
  }

  @Test
  public void assignFreshSchemaIdWhereNewSchemasFieldIdSameAsHighestFieldId() {
    Schema schemaOne = new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(Types.NestedField.required(2, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(Types.NestedField.required(3, "z", Types.LongType.get()));
    Schema newSchema = new Schema(Types.NestedField.required(3, "a", Types.LongType.get()));

    ViewMetadata metadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .setCurrentVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .schemaId(0)
                    .timestampMillis(System.currentTimeMillis())
                    .defaultNamespace(Namespace.empty())
                    .build(),
                schemaOne)
            .build();

    newSchema = ViewUtil.assignFreshIds(metadata, newSchema);
    assertThat(newSchema.highestFieldId()).isEqualTo(4);
  }
}
