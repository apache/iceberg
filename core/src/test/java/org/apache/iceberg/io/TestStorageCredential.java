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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestStorageCredential {
  @Test
  public void invalidPrefix() {
    assertThatThrownBy(() -> StorageCredential.create("", Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid prefix: must be non-empty");
  }

  @Test
  public void invalidConfig() {
    assertThatThrownBy(() -> StorageCredential.create("prefix", Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid config: must be non-empty");
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void serialization(TestHelpers.RoundTripSerializer<StorageCredential> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    // using a single element in the map will create a singleton map, which will work with Kryo.
    // However, creating two config elements will fail if the config in StorageCredential isn't a
    // SerializableMap
    StorageCredential credential =
        StorageCredential.create(
            "randomPrefix", ImmutableMap.of("token1", "storageToken1", "token2", "storageToken2"));
    assertThat(roundTripSerializer.apply(credential)).isEqualTo(credential);
  }
}
