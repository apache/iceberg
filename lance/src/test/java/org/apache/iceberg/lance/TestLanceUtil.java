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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link LanceUtil}. */
public class TestLanceUtil {

  @Test
  public void testAddExtension() {
    assertThat(LanceUtil.addExtension("data")).isEqualTo("data.lance");
    assertThat(LanceUtil.addExtension("data.lance")).isEqualTo("data.lance");
    assertThat(LanceUtil.addExtension("/path/to/file")).isEqualTo("/path/to/file.lance");
  }

  @Test
  public void testLanceProperties() {
    Map<String, String> props =
        ImmutableMap.of(
            "lance.fragment-size", "500000",
            "lance.compression", "lz4",
            "other.prop", "value",
            "write.format.default", "lance");

    Map<String, String> lanceProps = LanceUtil.lanceProperties(props);
    assertThat(lanceProps).hasSize(2);
    assertThat(lanceProps.get("fragment-size")).isEqualTo("500000");
    assertThat(lanceProps.get("compression")).isEqualTo("lz4");
  }

  @Test
  public void testFragmentSize() {
    Map<String, String> propsWithCustom = ImmutableMap.of("lance.fragment-size", "2000000");
    assertThat(LanceUtil.fragmentSize(propsWithCustom)).isEqualTo(2000000);

    Map<String, String> propsWithDefault = ImmutableMap.of("other.prop", "value");
    assertThat(LanceUtil.fragmentSize(propsWithDefault)).isEqualTo(LanceUtil.DEFAULT_FRAGMENT_SIZE);
  }

  @Test
  public void testCompression() {
    Map<String, String> propsWithCustom = ImmutableMap.of("lance.compression", "lz4");
    assertThat(LanceUtil.compression(propsWithCustom)).isEqualTo("lz4");

    Map<String, String> propsWithDefault = ImmutableMap.of("other.prop", "value");
    assertThat(LanceUtil.compression(propsWithDefault)).isEqualTo("zstd");
  }

  @Test
  public void testConstants() {
    assertThat(LanceUtil.LANCE_CONFIG_PREFIX).isEqualTo("lance.");
    assertThat(LanceUtil.LANCE_FRAGMENT_SIZE).isEqualTo("lance.fragment-size");
    assertThat(LanceUtil.LANCE_COMPRESSION).isEqualTo("lance.compression");
    assertThat(LanceUtil.DEFAULT_COMPRESSION).isEqualTo("zstd");
    assertThat(LanceUtil.DEFAULT_FRAGMENT_SIZE).isEqualTo(1024 * 1024);
  }

  @Test
  public void testEmptyProperties() {
    Map<String, String> emptyProps = ImmutableMap.of();
    assertThat(LanceUtil.lanceProperties(emptyProps)).isEmpty();
  }
}
