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

import java.io.IOException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestDeletionVectorStruct {

  @Test
  void testFieldAccess() {
    DeletionVectorStruct dv = new DeletionVectorStruct(DeletionVector.schema());

    dv.set(0, "s3://bucket/data/dv.puffin");
    dv.set(1, 256L);
    dv.set(2, 128L);
    dv.set(3, 42L);

    assertThat(dv.location()).isEqualTo("s3://bucket/data/dv.puffin");
    assertThat(dv.offset()).isEqualTo(256L);
    assertThat(dv.sizeInBytes()).isEqualTo(128L);
    assertThat(dv.cardinality()).isEqualTo(42L);
  }

  @Test
  void testCopy() {
    DeletionVectorStruct dv = new DeletionVectorStruct(DeletionVector.schema());

    dv.set(0, "s3://bucket/data/dv.puffin");
    dv.set(1, 256L);
    dv.set(2, 128L);
    dv.set(3, 42L);

    DeletionVectorStruct copy = dv.copy();

    assertThat(copy.location()).isEqualTo("s3://bucket/data/dv.puffin");
    assertThat(copy.offset()).isEqualTo(256L);
    assertThat(copy.sizeInBytes()).isEqualTo(128L);
    assertThat(copy.cardinality()).isEqualTo(42L);
  }

  @Test
  void testSize() {
    DeletionVectorStruct dv = new DeletionVectorStruct(DeletionVector.schema());
    assertThat(dv.size()).isEqualTo(4);
  }

  @Test
  void testProjectedStructLike() {
    // project only location (field ID 155) and cardinality (field ID 156)
    Types.StructType projection =
        Types.StructType.of(DeletionVector.LOCATION, DeletionVector.CARDINALITY);

    DeletionVectorStruct dv = new DeletionVectorStruct(projection);
    assertThat(dv.size()).isEqualTo(2);

    // projected position 0 maps to internal position 0 (location)
    // projected position 1 maps to internal position 3 (cardinality)
    dv.set(0, "s3://bucket/data/dv.puffin");
    dv.set(1, 42L);

    assertThat(dv.location()).isEqualTo("s3://bucket/data/dv.puffin");
    assertThat(dv.cardinality()).isEqualTo(42L);
    assertThat(dv.get(0, String.class)).isEqualTo("s3://bucket/data/dv.puffin");
    assertThat(dv.get(1, Long.class)).isEqualTo(42L);
  }

  @Test
  void testJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    DeletionVectorStruct dv = new DeletionVectorStruct(DeletionVector.schema());
    dv.set(0, "s3://bucket/data/dv.puffin");
    dv.set(1, 256L);
    dv.set(2, 128L);
    dv.set(3, 42L);

    DeletionVectorStruct deserialized = TestHelpers.roundTripSerialize(dv);

    assertThat(deserialized.location()).isEqualTo("s3://bucket/data/dv.puffin");
    assertThat(deserialized.offset()).isEqualTo(256L);
    assertThat(deserialized.sizeInBytes()).isEqualTo(128L);
    assertThat(deserialized.cardinality()).isEqualTo(42L);
  }

  @Test
  void testKryoSerializationRoundTrip() throws IOException {
    DeletionVectorStruct dv = new DeletionVectorStruct(DeletionVector.schema());
    dv.set(0, "s3://bucket/data/dv.puffin");
    dv.set(1, 256L);
    dv.set(2, 128L);
    dv.set(3, 42L);

    DeletionVectorStruct deserialized = TestHelpers.KryoHelpers.roundTripSerialize(dv);

    assertThat(deserialized.location()).isEqualTo("s3://bucket/data/dv.puffin");
    assertThat(deserialized.offset()).isEqualTo(256L);
    assertThat(deserialized.sizeInBytes()).isEqualTo(128L);
    assertThat(deserialized.cardinality()).isEqualTo(42L);
  }
}
