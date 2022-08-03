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
import com.aliyun.oss.OSSClientBuilder;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

public class AliyunClientFactories {

  private static final AliyunClientFactory ALIYUN_CLIENT_FACTORY_DEFAULT =
      new DefaultAliyunClientFactory();

  private AliyunClientFactories() {}

  public static AliyunClientFactory defaultFactory() {
    return ALIYUN_CLIENT_FACTORY_DEFAULT;
  }

  public static AliyunClientFactory from(Map<String, String> properties) {
    String factoryImpl =
        PropertyUtil.propertyAsString(
            properties,
            AliyunProperties.CLIENT_FACTORY,
            DefaultAliyunClientFactory.class.getName());
    return loadClientFactory(factoryImpl, properties);
  }

  /**
   * Load an implemented {@link AliyunClientFactory} based on the class name, and initialize it.
   *
   * @param impl the class name.
   * @param properties to initialize the factory.
   * @return an initialized {@link AliyunClientFactory}.
   */
  private static AliyunClientFactory loadClientFactory(
      String impl, Map<String, String> properties) {
    DynConstructors.Ctor<AliyunClientFactory> ctor;
    try {
      ctor = DynConstructors.builder(AliyunClientFactory.class).hiddenImpl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AliyunClientFactory, missing no-arg constructor: %s", impl),
          e);
    }

    AliyunClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AliyunClientFactory, %s does not implement AliyunClientFactory.",
              impl),
          e);
    }

    factory.initialize(properties);
    return factory;
  }

  static class DefaultAliyunClientFactory implements AliyunClientFactory {
    private AliyunProperties aliyunProperties;

    DefaultAliyunClientFactory() {}

    @Override
    public OSS newOSSClient() {
      Preconditions.checkNotNull(
          aliyunProperties,
          "Cannot create aliyun oss client before initializing the AliyunClientFactory.");

      return new OSSClientBuilder()
          .build(
              aliyunProperties.ossEndpoint(),
              aliyunProperties.accessKeyId(),
              aliyunProperties.accessKeySecret());
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
