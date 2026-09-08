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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestPartitionSpecEvolution {

  @Test
  void testCompatible() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));

    PartitionSpec spec1 = PartitionSpec.builderFor(schema).bucket("id", 10).build();
    PartitionSpec spec2 = PartitionSpec.builderFor(schema).bucket("id", 10).build();

    // Happy case, source ids and names match
    assertThat(PartitionSpecEvolution.checkCompatibility(spec1, spec2)).isTrue();
  }

  @Test
  void testNotCompatibleDifferentTransform() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));

    PartitionSpec spec1 = PartitionSpec.builderFor(schema).bucket("id", 10).build();
    // Same spec als spec1 but different number of buckets
    PartitionSpec spec2 = PartitionSpec.builderFor(schema).bucket("id", 23).build();

    assertThat(PartitionSpecEvolution.checkCompatibility(spec1, spec2)).isFalse();
  }

  @Test
  void testNotCompatibleMoreFields() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));

    PartitionSpec spec1 = PartitionSpec.builderFor(schema).bucket("id", 10).build();
    // Additional field
    PartitionSpec spec2 =
        PartitionSpec.builderFor(schema).bucket("id", 10).truncate("data", 1).build();

    assertThat(PartitionSpecEvolution.checkCompatibility(spec1, spec2)).isFalse();
  }

  @Test
  void testCompatibleWithNonMatchingSourceIds() {
    Schema schema1 =
        new Schema(
            // Use zero-based field ids
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));

    PartitionSpec spec1 = PartitionSpec.builderFor(schema1).bucket("id", 10).build();

    Schema schema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    // Same spec als spec1 but bound to a different schema
    PartitionSpec spec2 = PartitionSpec.builderFor(schema2).bucket("id", 10).build();

    // Compatible because the source names match
    assertThat(PartitionSpecEvolution.checkCompatibility(spec1, spec2)).isTrue();
  }

  @Test
  void testPartitionSpecEvolution() {
    Schema schema1 =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));

    PartitionSpec spec1 = PartitionSpec.builderFor(schema1).bucket("id", 10).build();

    Schema schema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    // Change num buckets
    PartitionSpec spec2 = PartitionSpec.builderFor(schema2).bucket("id", 23).build();

    assertThat(PartitionSpecEvolution.checkCompatibility(spec1, spec2)).isFalse();
    PartitionSpecEvolution.PartitionSpecChanges result =
        PartitionSpecEvolution.evolve(spec1, spec2);

    assertThat(result.termsToAdd().toString()).isEqualTo("[bucket[23](ref(name=\"id\"))]");
    assertThat(result.termsToRemove().toString()).isEqualTo("[bucket[10](ref(name=\"id\"))]");
  }

  @Test
  void testPartitionSpecEvolutionAddField() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));

    PartitionSpec spec1 = PartitionSpec.builderFor(schema).build();
    // Add field
    PartitionSpec spec2 = PartitionSpec.builderFor(schema).bucket("id", 23).build();

    assertThat(PartitionSpecEvolution.checkCompatibility(spec1, spec2)).isFalse();
    PartitionSpecEvolution.PartitionSpecChanges result =
        PartitionSpecEvolution.evolve(spec1, spec2);

    assertThat(result.termsToAdd().toString()).isEqualTo("[bucket[23](ref(name=\"id\"))]");
    assertThat(result.termsToRemove().toString()).isEqualTo("[]");
  }

  @Test
  void testPartitionSpecEvolutionRemoveField() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));

    PartitionSpec spec1 = PartitionSpec.builderFor(schema).bucket("id", 23).build();
    // Remove field
    PartitionSpec spec2 = PartitionSpec.builderFor(schema).build();

    assertThat(PartitionSpecEvolution.checkCompatibility(spec1, spec2)).isFalse();
    PartitionSpecEvolution.PartitionSpecChanges result =
        PartitionSpecEvolution.evolve(spec1, spec2);

    assertThat(result.termsToAdd().toString()).isEqualTo("[]");
    assertThat(result.termsToRemove().toString()).isEqualTo("[bucket[23](ref(name=\"id\"))]");
  }

  @Test
  void testPartitionSpecEvolutionWithNestedFields() {
    Schema schema1 =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                1,
                "data",
                Types.StructType.of(Types.NestedField.required(2, "str", Types.StringType.get()))));

    PartitionSpec spec1 = PartitionSpec.builderFor(schema1).bucket("data.str", 10).build();

    Schema schema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "data",
                Types.StructType.of(Types.NestedField.required(3, "str", Types.StringType.get()))));

    // Change num buckets
    PartitionSpec spec2 = PartitionSpec.builderFor(schema2).bucket("data.str", 23).build();

    assertThat(PartitionSpecEvolution.checkCompatibility(spec1, spec2)).isFalse();
    PartitionSpecEvolution.PartitionSpecChanges result =
        PartitionSpecEvolution.evolve(spec1, spec2);

    assertThat(result.termsToAdd().toString()).isEqualTo("[bucket[23](ref(name=\"data.str\"))]");
    assertThat(result.termsToRemove().toString()).isEqualTo("[bucket[10](ref(name=\"data.str\"))]");
  }
}
