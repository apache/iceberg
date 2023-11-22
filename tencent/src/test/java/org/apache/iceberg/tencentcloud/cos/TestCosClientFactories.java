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
package org.apache.iceberg.tencentcloud.cos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qcloud.cos.COS;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestCosClientFactories {

  @Test
  public void testLoadDefault() {
    assertThat(CosClientFactories.defaultFactory()).isEqualTo(CosClientFactories.defaultFactory());

    CosClientFactory defaultFactory = CosClientFactories.from(Maps.newHashMap());
    assertThat(defaultFactory).isInstanceOf(CosClientFactories.DefaultCosClientFactory.class);
    assertThat(defaultFactory.cosProperties().accessKeyId()).isNull();

    CosClientFactory defaultFactoryWithConfig =
        CosClientFactories.from(ImmutableMap.of(CosProperties.COS_USERINFO_SECRET_KEY, "key"));
    assertThat(defaultFactoryWithConfig)
        .isInstanceOf(CosClientFactories.DefaultCosClientFactory.class);
    assertThat("key").isEqualTo(defaultFactoryWithConfig.cosProperties().accessKeySecret());

    String clazz = "UnknownCosFactories";
    assertThatThrownBy(
            () -> CosClientFactories.from(ImmutableMap.of(CosProperties.CLIENT_FACTORY, clazz)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Cannot initialize CosClientFactory, missing no-arg constructor: %s", clazz));
  }

  @Test
  public void testLoadCustom() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CosProperties.CLIENT_FACTORY, CustomFactory.class.getName());
    assertThat(CosClientFactories.from(properties)).isInstanceOf(CustomFactory.class);
  }

  public static class CustomFactory implements CosClientFactory {

    CosProperties cosProperties;

    public CustomFactory() {}

    @Override
    public COS newCosClient() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.cosProperties = new CosProperties(properties);
    }

    @Override
    public CosProperties cosProperties() {
      return cosProperties;
    }
  }
}
