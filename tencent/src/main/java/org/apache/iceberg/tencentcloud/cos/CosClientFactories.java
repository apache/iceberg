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

import com.qcloud.cos.COS;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

public class CosClientFactories {

  private static final CosClientFactory COS_CLIENT_FACTORY_DEFAULT = new DefaultCosClientFactory();

  private CosClientFactories() {}

  public static CosClientFactory defaultFactory() {
    return COS_CLIENT_FACTORY_DEFAULT;
  }

  public static CosClientFactory from(Map<String, String> properties) {
    String factoryImpl =
        PropertyUtil.propertyAsString(
            properties, CosProperties.CLIENT_FACTORY, DefaultCosClientFactory.class.getName());
    return loadClientFactory(factoryImpl, properties);
  }

  private static CosClientFactory loadClientFactory(String impl, Map<String, String> properties) {
    DynConstructors.Ctor<CosClientFactory> ctor;
    try {
      ctor = DynConstructors.builder(CosClientFactory.class).hiddenImpl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize CosClientFactory, missing no-arg constructor: %s", impl),
          e);
    }

    CosClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize CosClientFactory, %s does not implement AliyunClientFactory.",
              impl),
          e);
    }

    factory.initialize(properties);
    return factory;
  }

  static class DefaultCosClientFactory implements CosClientFactory {
    private CosProperties cosProperties;

    DefaultCosClientFactory() {}

    @Override
    public COS newCosClient() {
      Preconditions.checkNotNull(
          cosProperties, "Cannot create cos client before initializing the CosClientFactory.");

      COSCredentials cred =
          new BasicCOSCredentials(cosProperties.accessKeyId(), cosProperties.accessKeySecret());
      ClientConfig clientConfig = new ClientConfig(new Region(cosProperties.cosRegion()));
      return new COSClient(cred, clientConfig);
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
