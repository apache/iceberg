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

package org.apache.iceberg.aws;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;


public class AwsClientFactoriesTest {

  @Test
  public void testLoadDefault() {
    Assert.assertEquals("default client should be singleton",
        AwsClientFactories.defaultFactory(), AwsClientFactories.defaultFactory());

    Assert.assertTrue("should load default when not configured",
        AwsClientFactories.from(Maps.newHashMap()) instanceof AwsClientFactories.DefaultAwsClientFactory);
  }

  @Test
  public void testLoadCustom() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsClientFactories.CLIENT_FACTORY_CONFIG_KEY, CustomFactory.class.getName());
    Assert.assertTrue("should load custom class",
        AwsClientFactories.from(properties) instanceof CustomFactory);
  }

  public static class CustomFactory implements AwsClientFactory {

    public CustomFactory() {
    }

    @Override
    public S3Client s3() {
      return null;
    }

    @Override
    public GlueClient glue() {
      return null;
    }

    @Override
    public KmsClient kms() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> properties) {

    }
  }

}
