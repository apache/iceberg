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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestPartitionData {

  private static final Types.StructType PARTITION_TYPE =
      Types.StructType.of(
          Types.NestedField.required(1000, "id", Types.IntegerType.get()),
          Types.NestedField.optional(1001, "data", Types.StringType.get()));

  private static final Types.StructType BINARY_PARTITION_TYPE =
      Types.StructType.of(
          Types.NestedField.required(1000, "id", Types.IntegerType.get()),
          Types.NestedField.optional(1001, "binary", Types.BinaryType.get()),
          Types.NestedField.optional(1002, "fixed", Types.FixedType.ofLength(4)));

  private static PartitionData newData(int id, String data) {
    PartitionData partitionData = new PartitionData(PARTITION_TYPE);
    partitionData.set(0, id);
    partitionData.set(1, data);
    return partitionData;
  }

  private static PartitionData newBinaryData(int id, byte[] binary, byte[] fixed) {
    PartitionData partitionData = new PartitionData(BINARY_PARTITION_TYPE);
    partitionData.set(0, id);
    partitionData.set(1, binary);
    partitionData.set(2, fixed);
    return partitionData;
  }

  @Test
  public void copyOfEqualValuesReturnsSameInstance() {
    PartitionData first = newData(1, "a").copy();
    PartitionData second = newData(1, "a").copy();

    assertThat(first).isEqualTo(second);
    assertThat(second).isSameAs(first);
  }

  @Test
  public void copyOfDifferentValuesReturnsDistinctInstances() {
    PartitionData first = newData(1, "a").copy();
    PartitionData second = newData(2, "a").copy();

    assertThat(first).isNotEqualTo(second);
    assertThat(second).isNotSameAs(first);
  }

  @Test
  public void copyForOfEqualValuesReturnsSameInstance() {
    PartitionData template = new PartitionData(PARTITION_TYPE);

    PartitionData first = template.copyFor(newData(1, "a"));
    PartitionData second = template.copyFor(newData(1, "a"));

    assertThat(first).isEqualTo(second);
    assertThat(second).isSameAs(first);
  }

  @Test
  public void mutatingSourceAfterCopyDoesNotAffectInternedInstance() {
    PartitionData source = newData(1, "a");
    PartitionData copy = source.copy();

    // mutate the source (as a reused builder scratch instance would be between files)
    source.set(0, 2);
    source.set(1, "b");

    assertThat(copy.get(0, Integer.class)).isEqualTo(1);
    assertThat(copy.get(1, String.class)).isEqualTo("a");
  }

  @Test
  public void emptyPartitionCopyIsUnaffectedByInterning() {
    assertThat(PartitionData.EMPTY.copy()).isSameAs(PartitionData.EMPTY);
  }

  @Test
  public void copyOfEqualBinaryValuesReturnsSameInstance() {
    PartitionData first = newBinaryData(1, new byte[] {1, 2}, new byte[] {3, 4, 5, 6}).copy();
    PartitionData second = newBinaryData(1, new byte[] {1, 2}, new byte[] {3, 4, 5, 6}).copy();

    assertThat(first).isEqualTo(second);
    assertThat(second).isSameAs(first);
  }

  @Test
  public void copyOfDifferentBinaryValuesReturnsDistinctInstances() {
    PartitionData first = newBinaryData(1, new byte[] {1, 2}, new byte[] {3, 4, 5, 6}).copy();
    PartitionData second = newBinaryData(1, new byte[] {1, 3}, new byte[] {3, 4, 5, 6}).copy();

    assertThat(first).isNotEqualTo(second);
    assertThat(second).isNotSameAs(first);
  }

  @Test
  public void independentTemplatesForEqualPartitionTypesShareSchema() {
    // two unrelated scans/readers build their own template for the same partition type; they
    // must not end up with two different (though structurally equal) Avro schema instances,
    // since callers like PartitionsTable rely on reference equality of the schema across a scan
    PartitionData first = new PartitionData(PARTITION_TYPE);
    PartitionData second = new PartitionData(PARTITION_TYPE);

    assertThat(first.getSchema()).isSameAs(second.getSchema());
  }

  @Test
  public void copyForFromIndependentTemplatesSharesSchema() {
    PartitionData templateOne = new PartitionData(PARTITION_TYPE);
    PartitionData templateTwo = new PartitionData(PARTITION_TYPE);

    // different partition values, so the interner cannot collapse these to the same instance
    PartitionData first = templateOne.copyFor(newData(1, "a"));
    PartitionData second = templateTwo.copyFor(newData(2, "b"));

    assertThat(first).isNotSameAs(second);
    assertThat(first.getSchema()).isSameAs(second.getSchema());
  }
}
