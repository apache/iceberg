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
package org.apache.iceberg.huaweicloud;

import com.obs.services.ObsClient;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

public class TestHuaweicloudClientFactories {

  @Test
  public void testLoadDefault() {
    Assert.assertEquals(
        "Default client should be singleton",
        HuaweicloudClientFactories.defaultFactory(),
        HuaweicloudClientFactories.defaultFactory());

    HuaweicloudClientFactory defaultFactory = HuaweicloudClientFactories.from(Maps.newHashMap());
    Assert.assertTrue(
        "Should load default when factory impl not configured",
        defaultFactory instanceof HuaweicloudClientFactories.DefaultHuaweicloudClientFactory);
    Assert.assertNull(
        "Should have no Huaweicloud properties set",
        defaultFactory.huaweicloudProperties().accessKeyId());

    HuaweicloudClientFactory defaultFactoryWithConfig =
        HuaweicloudClientFactories.from(
            ImmutableMap.of(HuaweicloudProperties.CLIENT_ACCESS_KEY_ID, "key"));
    Assert.assertTrue(
        "Should load default when factory impl not configured",
        defaultFactoryWithConfig
            instanceof HuaweicloudClientFactories.DefaultHuaweicloudClientFactory);
    Assert.assertEquals(
        "Should have access key set",
        "key",
        defaultFactoryWithConfig.huaweicloudProperties().accessKeyId());
  }

  @Test
  public void testLoadCustom() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HuaweicloudProperties.CLIENT_FACTORY, CustomFactory.class.getName());
    Assert.assertTrue(
        "Should load custom class",
        HuaweicloudClientFactories.from(properties) instanceof CustomFactory);
  }

  public static class CustomFactory implements HuaweicloudClientFactory {

    HuaweicloudProperties huaweicloudProperties;

    public CustomFactory() {}

    @Override
    public ObsClient newOBSClient() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.huaweicloudProperties = new HuaweicloudProperties(properties);
    }

    @Override
    public HuaweicloudProperties huaweicloudProperties() {
      return huaweicloudProperties;
    }
  }
}
