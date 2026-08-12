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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.signing.ImmutableRemoteSigningConfig;
import org.apache.iceberg.rest.signing.RemoteSigningConfig;
import org.junit.jupiter.api.Test;

class TestSerializableRemoteSigningConfig {

  @Test
  void copyOfEmpty() {
    RemoteSigningConfig empty = ImmutableRemoteSigningConfig.builder().build();
    SerializableRemoteSigningConfig copy = SerializableRemoteSigningConfig.copyOf(empty);
    assertThat(copy.properties()).isEmpty();
    assertThat(copy.headers()).isEmpty();
    assertThat(copy.isEmpty()).isTrue();
  }

  @Test
  void copyOfWithProperties() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder().properties(Map.of("k1", "v1", "k2", "v2")).build();
    SerializableRemoteSigningConfig copy = SerializableRemoteSigningConfig.copyOf(config);
    assertThat(copy.properties()).containsExactlyInAnyOrderEntriesOf(config.properties());
    assertThat(copy.headers()).isEmpty();
    assertThat(copy.isEmpty()).isFalse();
  }

  @Test
  void copyOfWithHeaders() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putHeaders("Authorization", List.of("Bearer token"))
            .putHeaders("X-Multi", List.of("a", "b"))
            .build();
    SerializableRemoteSigningConfig copy = SerializableRemoteSigningConfig.copyOf(config);
    assertThat(copy.headers()).containsKey("Authorization");
    assertThat(copy.headers().get("Authorization")).containsExactly("Bearer token");
    assertThat(copy.headers().get("X-Multi")).containsExactly("a", "b");
    assertThat(copy.properties()).isEmpty();
  }

  @Test
  void copyOfWithPropertiesAndHeaders() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putProperties("prop", "val")
            .putHeaders("H", List.of("hval"))
            .build();
    SerializableRemoteSigningConfig copy = SerializableRemoteSigningConfig.copyOf(config);
    assertThat(copy.properties()).containsEntry("prop", "val");
    assertThat(copy.headers().get("H")).containsExactly("hval");
  }

  @Test
  void immutableConfigLazyInit() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder().putProperties("k", "v").build();
    SerializableRemoteSigningConfig copy = SerializableRemoteSigningConfig.copyOf(config);

    RemoteSigningConfig immutable1 = copy.immutableConfig();
    RemoteSigningConfig immutable2 = copy.immutableConfig();
    assertThat(immutable1).isSameAs(immutable2);
    assertThat(immutable1.properties()).containsEntry("k", "v");
  }

  @Test
  void javaSerializationRoundTrip() throws Exception {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putProperties("k1", "v1")
            .putHeaders("H", List.of("a", "b"))
            .build();
    SerializableRemoteSigningConfig original = SerializableRemoteSigningConfig.copyOf(config);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(original);
    }

    SerializableRemoteSigningConfig deserialized;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      deserialized = (SerializableRemoteSigningConfig) ois.readObject();
    }

    assertThat(deserialized.properties()).containsExactlyInAnyOrderEntriesOf(original.properties());
    assertThat(deserialized.headers().get("H"))
        .containsExactlyElementsOf(original.headers().get("H"));

    // immutableConfig is transient — must be rebuilt after deserialization
    RemoteSigningConfig rebuilt = deserialized.immutableConfig();
    assertThat(rebuilt).isEqualTo(config);
  }
}
