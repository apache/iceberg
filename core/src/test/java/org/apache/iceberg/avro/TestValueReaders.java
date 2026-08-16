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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class TestValueReaders {
  @Test
  void rowIdPreservesPhysicalValueWithoutBase() throws IOException {
    ValueReader<Long> reader = ValueReaders.rowIds(null, ValueReaders.constant(34L));

    assertThat(reader.read(null, null)).isEqualTo(34L);
  }

  @Test
  void lastUpdatedPreservesPhysicalValueWithoutDataSequence() throws IOException {
    ValueReader<Long> reader = ValueReaders.lastUpdated(10L, null, ValueReaders.constant(5L));

    assertThat(reader.read(null, null)).isEqualTo(5L);
  }

  @Test
  void lastUpdatedInheritsWithoutBase() throws IOException {
    ValueReader<Long> reader = ValueReaders.lastUpdated(null, 5L, null);

    assertThat(reader.read(null, null)).isEqualTo(5L);
  }
}
