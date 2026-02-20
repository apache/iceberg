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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.connect.channel.CommitterImpl;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

public class TestCommitterFactory {

  private static final Map<String, String> BASE_CONFIG =
      ImmutableMap.of(
          "name",
          "test-connector",
          "iceberg.tables",
          "test_table",
          "iceberg.catalog.catalog-impl",
          "org.apache.iceberg.inmemory.InMemoryCatalog");

  @Test
  public void testCreateDefaultCommitter() {
    // When no committer.class is configured, should return CommitterImpl
    IcebergSinkConfig config = new IcebergSinkConfig(BASE_CONFIG);

    Committer committer = CommitterFactory.createCommitter(config);

    assertThat(committer).isNotNull();
    assertThat(committer).isInstanceOf(CommitterImpl.class);
  }

  @Test
  public void testCreateCustomCommitter() {
    // When valid custom committer class is configured, should instantiate it
    Map<String, String> configWithCustomCommitter =
        ImmutableMap.<String, String>builder()
            .putAll(BASE_CONFIG)
            .put("iceberg.committer.class", "org.apache.iceberg.connect.TestCustomCommitter")
            .build();

    IcebergSinkConfig config = new IcebergSinkConfig(configWithCustomCommitter);

    Committer committer = CommitterFactory.createCommitter(config);

    assertThat(committer).isNotNull();
    assertThat(committer).isInstanceOf(TestCustomCommitter.class);
  }

  @Test
  public void testCreateCommitterWithWhitespace() {
    // Should handle whitespace in class name
    Map<String, String> configWithWhitespace =
        ImmutableMap.<String, String>builder()
            .putAll(BASE_CONFIG)
            .put("iceberg.committer.class", "  org.apache.iceberg.connect.TestCustomCommitter  ")
            .build();

    IcebergSinkConfig config = new IcebergSinkConfig(configWithWhitespace);

    Committer committer = CommitterFactory.createCommitter(config);

    assertThat(committer).isNotNull();
    assertThat(committer).isInstanceOf(TestCustomCommitter.class);
  }

  @Test
  public void testCreateCommitterClassNotFound() {
    // Should throw ConfigException when class doesn't exist
    Map<String, String> configWithInvalidClass =
        ImmutableMap.<String, String>builder()
            .putAll(BASE_CONFIG)
            .put("iceberg.committer.class", "com.example.NonExistentCommitter")
            .build();

    IcebergSinkConfig config = new IcebergSinkConfig(configWithInvalidClass);

    assertThatThrownBy(() -> CommitterFactory.createCommitter(config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Committer class not found")
        .hasMessageContaining("com.example.NonExistentCommitter");
  }

  @Test
  public void testCreateCommitterNotImplementingInterface() {
    // Should throw ConfigException when class doesn't implement Committer
    Map<String, String> configWithWrongClass =
        ImmutableMap.<String, String>builder()
            .putAll(BASE_CONFIG)
            .put("iceberg.committer.class", "java.lang.String")
            .build();

    IcebergSinkConfig config = new IcebergSinkConfig(configWithWrongClass);

    assertThatThrownBy(() -> CommitterFactory.createCommitter(config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("must implement org.apache.iceberg.connect.Committer");
  }

  @Test
  public void testCreateCommitterNoPublicConstructor() {
    // Should throw ConfigException when class doesn't have public no-arg constructor
    Map<String, String> configWithNoConstructor =
        ImmutableMap.<String, String>builder()
            .putAll(BASE_CONFIG)
            .put(
                "iceberg.committer.class",
                "org.apache.iceberg.connect.TestCommitterFactory$CommitterWithoutPublicConstructor")
            .build();

    IcebergSinkConfig config = new IcebergSinkConfig(configWithNoConstructor);

    assertThatThrownBy(() -> CommitterFactory.createCommitter(config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Cannot find constructor");
  }

  @Test
  public void testEmptyCommitterClassUsesDefault() {
    // Empty string should use default committer
    Map<String, String> configWithEmptyClass =
        ImmutableMap.<String, String>builder()
            .putAll(BASE_CONFIG)
            .put("iceberg.committer.class", "")
            .build();

    IcebergSinkConfig config = new IcebergSinkConfig(configWithEmptyClass);

    Committer committer = CommitterFactory.createCommitter(config);

    assertThat(committer).isNotNull();
    assertThat(committer).isInstanceOf(CommitterImpl.class);
  }

  @Test
  public void testNullCommitterClassUsesDefault() {
    // Null (not configured) should use default committer
    IcebergSinkConfig config = new IcebergSinkConfig(BASE_CONFIG);

    assertThat(config.committerClass()).isNull();

    Committer committer = CommitterFactory.createCommitter(config);

    assertThat(committer).isNotNull();
    assertThat(committer).isInstanceOf(CommitterImpl.class);
  }

  // Test helper class - committer without public constructor
  private static class CommitterWithoutPublicConstructor implements Committer {
    private CommitterWithoutPublicConstructor() {}

    @Override
    public void onTaskStarted(
        IcebergSinkConfig config, org.apache.kafka.connect.sink.SinkTaskContext context) {}

    @Override
    public void onPartitionsAdded(
        java.util.Collection<org.apache.kafka.common.TopicPartition> addedPartitions) {}

    @Override
    public void onPartitionsRemoved(
        java.util.Collection<org.apache.kafka.common.TopicPartition> closedPartitions) {}

    @Override
    public void onTaskStopped() {}

    @Override
    public void save(java.util.Collection<org.apache.kafka.connect.sink.SinkRecord> sinkRecords) {}
  }
}
