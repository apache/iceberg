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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

class TestSerializationUtil {

  @Test
  void bytesRoundTripPreservesValue() {
    String original = "s3://bucket/table/metadata/v1.metadata.json";
    byte[] bytes = SerializationUtil.serializeToBytes(original);
    String roundTripped = SerializationUtil.deserializeFromBytes(bytes);
    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void bytesRoundTripPreservesMapContents() {
    Map<String, Integer> original = Maps.newHashMap();
    original.put("added-records", 42);
    original.put("total-files", 7);

    byte[] bytes = SerializationUtil.serializeToBytes(original);
    Map<String, Integer> roundTripped = SerializationUtil.deserializeFromBytes(bytes);
    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void deserializeFromBytesReturnsNullForNullInput() {
    Object result = SerializationUtil.deserializeFromBytes(null);
    assertThat(result).isNull();
  }

  @Test
  void bytesRoundTripPreservesNull() {
    // serializeToBytes(null) writes a real serialized null (not an empty array), so this exercises
    // the write-and-read-back path rather than the null-input short circuit above.
    byte[] bytes = SerializationUtil.serializeToBytes(null);
    assertThat(bytes).isNotNull();

    Object roundTripped = SerializationUtil.deserializeFromBytes(bytes);
    assertThat(roundTripped).isNull();
  }

  @Test
  void base64RoundTripPreservesValue() {
    String original = "s3://bucket/table/metadata/v1.metadata.json";
    String encoded = SerializationUtil.serializeToBase64(original);
    String roundTripped = SerializationUtil.deserializeFromBase64(encoded);
    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void deserializeFromBase64ReturnsNullForNullInput() {
    Object result = SerializationUtil.deserializeFromBase64(null);
    assertThat(result).isNull();
  }

  @Test
  void base64RoundTripHandlesMimeLineWrapping() {
    // A payload whose base64 exceeds 76 characters forces the MIME encoder to insert line breaks;
    // the round trip verifies the MIME decoder tolerates that wrapping.
    String original = "a".repeat(1000);
    String encoded = SerializationUtil.serializeToBase64(original);
    assertThat(encoded).contains("\r\n");

    String roundTripped = SerializationUtil.deserializeFromBase64(encoded);
    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void serializeToBytesAppliesCustomConfSerializerToHadoopConfigurable() {
    Configuration conf = new Configuration(false);
    conf.set("test.key", "test.value");
    HadoopConfigurableFixture configurable = new HadoopConfigurableFixture(conf);

    // Return a distinctive supplier whose configuration carries a marker key. If serializeToBytes
    // ignored our serializer's result and built its own, the marker would be absent after the
    // round trip, so asserting on it proves our output is what actually got serialized.
    Function<Configuration, SerializableSupplier<Configuration>> confSerializer =
        c -> {
          Configuration marked = new Configuration(c);
          marked.set("custom.serializer.marker", "applied");
          return new SerializableConfiguration(marked);
        };

    byte[] bytes = SerializationUtil.serializeToBytes(configurable, confSerializer);
    HadoopConfigurableFixture roundTripped = SerializationUtil.deserializeFromBytes(bytes);

    assertThat(configurable.serializeConfWithInvoked)
        .as("serializeConfWith should be called for a HadoopConfigurable object")
        .isTrue();
    assertThat(roundTripped.getConf().get("custom.serializer.marker"))
        .as(
            "the configuration produced by the provided confSerializer should be the one serialized")
        .isEqualTo("applied");
  }

  @Test
  void serializeToBytesWrapsIOException() {
    // A non-Serializable object makes ObjectOutputStream throw NotSerializableException.
    Object notSerializable = new Object();
    assertThatThrownBy(() -> SerializationUtil.serializeToBytes(notSerializable))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessage("Failed to serialize object")
        .hasCauseInstanceOf(NotSerializableException.class);
  }

  @Test
  void deserializeFromBytesWrapsIOException() {
    // Bytes that are not a valid object stream make ObjectInputStream throw an IOException.
    byte[] corrupted = {0, 1, 2, 3};
    assertThatThrownBy(() -> SerializationUtil.deserializeFromBytes(corrupted))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessage("Failed to deserialize object")
        .hasCauseInstanceOf(StreamCorruptedException.class);
  }

  @Test
  void hadoopConfigurableRoundTripPreservesConfiguration() {
    Configuration conf = new Configuration(false);
    conf.set("test.key", "test.value");
    HadoopConfigurableFixture configurable = new HadoopConfigurableFixture(conf);

    // The fixture holds a live, non-serializable Configuration, so this round trip only succeeds
    // because serializeToBytes routes HadoopConfigurable objects through serializeConfWith. If
    // that branch regresses, serialization fails here instead of silently passing.
    byte[] bytes = SerializationUtil.serializeToBytes(configurable);
    HadoopConfigurableFixture roundTripped = SerializationUtil.deserializeFromBytes(bytes);

    assertThat(configurable.serializeConfWithInvoked)
        .as("serializeToBytes should route HadoopConfigurable objects through serializeConfWith")
        .isTrue();
    assertThat(roundTripped.getConf().get("test.key")).isEqualTo("test.value");
  }

  private static class HadoopConfigurableFixture implements HadoopConfigurable, Serializable {
    // Not transient on purpose: a live Configuration is not Serializable, so serializing this
    // fixture fails with NotSerializableException unless serializeConfWith first replaces it with
    // a serializable supplier. That mirrors real HadoopConfigurable objects (e.g. HadoopFileIO)
    // and is exactly the contract SerializationUtil's HadoopConfigurable branch fulfills.
    private Configuration conf;
    private SerializableSupplier<Configuration> serializableConf;
    private transient boolean serializeConfWithInvoked = false;

    HadoopConfigurableFixture(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return serializableConf != null ? serializableConf.get() : conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public void serializeConfWith(
        Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
      this.serializeConfWithInvoked = true;
      this.serializableConf = confSerializer.apply(getConf());
      // Drop the non-serializable reference so the fixture can be serialized.
      this.conf = null;
    }
  }
}
