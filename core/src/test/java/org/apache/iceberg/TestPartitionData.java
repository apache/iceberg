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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.ref.WeakReference;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
  public void copyOfEqualValuesSharesBackingArray() {
    PartitionData first = newData(1, "a").copy();
    PartitionData second = newData(1, "a").copy();

    assertThat(first).isEqualTo(second);
    // the PartitionData wrapper is never shared - only the values it wraps are
    assertThat(first).isNotSameAs(second);
    assertThat(first.rawData()).isSameAs(second.rawData());
  }

  @Test
  public void copyOfDifferentValuesHasDistinctBackingArrays() {
    PartitionData first = newData(1, "a").copy();
    PartitionData second = newData(2, "a").copy();

    assertThat(first).isNotEqualTo(second);
    assertThat(first.rawData()).isNotSameAs(second.rawData());
  }

  @Test
  public void copyForOfEqualValuesSharesBackingArray() {
    PartitionData template = new PartitionData(PARTITION_TYPE);

    PartitionData first = template.copyFor(newData(1, "a"));
    PartitionData second = template.copyFor(newData(1, "a"));

    assertThat(first).isEqualTo(second);
    assertThat(first).isNotSameAs(second);
    assertThat(first.rawData()).isSameAs(second.rawData());
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
  public void copyOfEqualBinaryValuesSharesBackingArray() {
    PartitionData first = newBinaryData(1, new byte[] {1, 2}, new byte[] {3, 4, 5, 6}).copy();
    PartitionData second = newBinaryData(1, new byte[] {1, 2}, new byte[] {3, 4, 5, 6}).copy();

    assertThat(first).isEqualTo(second);
    assertThat(first.rawData()).isSameAs(second.rawData());
  }

  @Test
  public void copyOfDifferentBinaryValuesHasDistinctBackingArrays() {
    PartitionData first = newBinaryData(1, new byte[] {1, 2}, new byte[] {3, 4, 5, 6}).copy();
    PartitionData second = newBinaryData(1, new byte[] {1, 3}, new byte[] {3, 4, 5, 6}).copy();

    assertThat(first).isNotEqualTo(second);
    assertThat(first.rawData()).isNotSameAs(second.rawData());
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

    // different partition values, so the values interner cannot collapse these to one instance
    PartitionData first = templateOne.copyFor(newData(1, "a"));
    PartitionData second = templateTwo.copyFor(newData(2, "b"));

    assertThat(first.rawData()).isNotSameAs(second.rawData());
    assertThat(first.getSchema()).isSameAs(second.getSchema());
  }

  @Test
  public void copyResultRejectsSet() {
    PartitionData copy = newData(1, "a").copy();

    assertThatThrownBy(() -> copy.set(0, 2))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("shared");
  }

  @Test
  public void copyResultRejectsPut() {
    PartitionData copy = newData(1, "a").copy();

    assertThatThrownBy(() -> copy.put(0, 2))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("shared");
  }

  @Test
  public void copyResultRejectsClear() {
    PartitionData copy = newData(1, "a").copy();

    assertThatThrownBy(copy::clear)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("shared");
  }

  @Test
  public void copyForResultRejectsMutation() {
    PartitionData template = new PartitionData(PARTITION_TYPE);
    PartitionData copy = template.copyFor(newData(1, "a"));

    assertThatThrownBy(() -> copy.set(0, 2)).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void copyIsEvictedOnceUnreferenced() throws InterruptedException {
    // regression test: a self-referencing cache (key == value, like interning) only actually
    // cleans up if the interned instance itself is weakly referenced. A Cache<Values, Values>
    // with Caffeine's weakValues() looks like it would work but does not - the key slot holds a
    // strong reference to the same object the value slot only weakly references, so nothing is
    // ever collected. This asserts the real, observable cleanup behavior of the backing array
    // rather than just dedup semantics, so a future change back to that pattern fails here
    // instead of silently reintroducing an unbounded leak.
    WeakReference<Object[]> ref = new WeakReference<>(newData(1, "a").copy().rawData());

    for (int i = 0; i < 10 && ref.get() != null; i++) {
      System.gc();
      Thread.sleep(50);
    }

    assertThat(ref.get()).isNull();
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void copyResultSurvivesRoundTripSerialization(
      TestHelpers.RoundTripSerializer<PartitionData> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    // Values wraps the backing array so it can be interned (arrays can't override
    // equals()/hashCode()); it must itself be Serializable, since PartitionData - and anything
    // holding a copy()'d one, like Spark/Flink shipping tasks to executors - is
    PartitionData copy = newData(1, "a").copy();

    PartitionData roundTripped = roundTripSerializer.apply(copy);

    assertThat(roundTripped).isEqualTo(copy);
    assertThat(roundTripped.get(0, Integer.class)).isEqualTo(1);
    assertThat(roundTripped.get(1, String.class)).isEqualTo("a");
  }

  @Test
  public void avroReflectionConstructorConvergesToCanonicalSchema() {
    PartitionData template = new PartitionData(PARTITION_TYPE);

    // a schema that is structurally equivalent to the template's, but deliberately not the
    // cached, canonical instance - simulates Avro handing a freshly-parsed schema object to the
    // SpecificData.SchemaConstructable constructor when reading a manifest
    Schema freshlyConverted =
        AvroSchemaUtil.convert(PARTITION_TYPE, PartitionData.class.getName() + "Copy");
    assertThat(freshlyConverted).isNotSameAs(template.getSchema());

    PartitionData fromSchema = new PartitionData(freshlyConverted);

    assertThat(fromSchema.getSchema()).isSameAs(template.getSchema());
  }
}
