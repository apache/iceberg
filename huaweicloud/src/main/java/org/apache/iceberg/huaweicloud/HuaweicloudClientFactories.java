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

import com.obs.services.IObsClient;
import com.obs.services.ObsClient;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

public class HuaweicloudClientFactories {

  private static final HuaweicloudClientFactory HUAWEICLOUD_CLIENT_FACTORY_DEFAULT =
      new DefaultHuaweicloudClientFactory();

  private HuaweicloudClientFactories() {}

  public static HuaweicloudClientFactory defaultFactory() {
    return HUAWEICLOUD_CLIENT_FACTORY_DEFAULT;
  }

  public static HuaweicloudClientFactory from(Map<String, String> properties) {
    String factoryImpl =
        PropertyUtil.propertyAsString(
            properties,
            HuaweicloudProperties.CLIENT_FACTORY,
            DefaultHuaweicloudClientFactory.class.getName());
    return loadClientFactory(factoryImpl, properties);
  }

  /**
   * Load an implemented {@link HuaweicloudClientFactory} based on the class name, and initialize
   * it.
   *
   * @param impl the class name.
   * @param properties to initialize the factory.
   * @return an initialized {@link HuaweicloudClientFactory}.
   */
  private static HuaweicloudClientFactory loadClientFactory(
      String impl, Map<String, String> properties) {
    DynConstructors.Ctor<HuaweicloudClientFactory> ctor;
    try {
      ctor =
          DynConstructors.builder(HuaweicloudClientFactory.class).hiddenImpl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize HuaweicloudClientFactory, missing no-arg constructor: %s", impl),
          e);
    }

    HuaweicloudClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize HuaweicloudClientFactory, %s does not implement HuaweicloudClientFactory.",
              impl),
          e);
    }

    factory.initialize(properties);
    return factory;
  }

  static class DefaultHuaweicloudClientFactory implements HuaweicloudClientFactory {
    private HuaweicloudProperties huaweicloudProperties;

    DefaultHuaweicloudClientFactory() {}

    @Override
    public IObsClient newOBSClient() {
      Preconditions.checkNotNull(
          huaweicloudProperties,
          "Cannot create huaweicloud obs client before initializing the HuaweicloudClientFactory.");

      return new ObsClient(
          huaweicloudProperties.accessKeyId(),
          huaweicloudProperties.accessKeySecret(),
          huaweicloudProperties.obsEndpoint());
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
