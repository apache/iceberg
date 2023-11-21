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
package org.apache.iceberg.aliyun;

import com.aliyun.oss.OSS;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAliyunClientFactories {

  @Test
  public void testLoadDefault() {

    Assertions.assertThat(AliyunClientFactories.defaultFactory())
        .as("Default client should be singleton")
        .isEqualTo(AliyunClientFactories.defaultFactory());

    AliyunClientFactory defaultFactory = AliyunClientFactories.from(Maps.newHashMap());

    Assertions.assertThat(defaultFactory)
        .as("Should load default when factory impl not configured")
        .isInstanceOf(AliyunClientFactories.DefaultAliyunClientFactory.class);

    Assertions.assertThat(defaultFactory.aliyunProperties().accessKeyId())
        .as("Should have no Aliyun properties set")
        .isNull();

    AliyunClientFactory defaultFactoryWithConfig =
        AliyunClientFactories.from(ImmutableMap.of(AliyunProperties.CLIENT_ACCESS_KEY_ID, "key"));

    Assertions.assertThat(defaultFactoryWithConfig)
        .as("Should load default when factory impl not configured")
        .isInstanceOf(AliyunClientFactories.DefaultAliyunClientFactory.class);

    Assertions.assertThat(defaultFactoryWithConfig.aliyunProperties().accessKeyId())
        .as("Should have access key set")
        .isEqualTo("key");
  }

  @Test
  public void testLoadCustom() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AliyunProperties.CLIENT_FACTORY, CustomFactory.class.getName());

    Assertions.assertThat(AliyunClientFactories.from(properties))
        .as("Should load custom class")
        .isInstanceOf(CustomFactory.class);
  }

  public static class CustomFactory implements AliyunClientFactory {

    AliyunProperties aliyunProperties;

    public CustomFactory() {}

    @Override
    public OSS newOSSClient() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.aliyunProperties = new AliyunProperties(properties);
    }

    @Override
    public AliyunProperties aliyunProperties() {
      return aliyunProperties;
    }
  }
}
