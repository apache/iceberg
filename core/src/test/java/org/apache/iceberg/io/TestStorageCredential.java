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
import org.junit.jupiter.api.Test;

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

  @Test
  public void kryoSerDe() throws IOException {
    StorageCredential credential =
        StorageCredential.create("randomPrefix", Map.of("token", "storageToken"));
    assertThat(TestHelpers.KryoHelpers.roundTripSerialize(credential)).isEqualTo(credential);
  }

  @Test
  public void javaSerDe() throws IOException, ClassNotFoundException {
    StorageCredential credential =
        StorageCredential.create("randomPrefix", Map.of("token", "storageToken"));
    assertThat(TestHelpers.roundTripSerialize(credential)).isEqualTo(credential);
  }
}
