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
package org.apache.iceberg.dell;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.util.PropertyUtil;

public class DellClientFactories {

  private DellClientFactories() {}

  public static DellClientFactory from(Map<String, String> properties) {
    String factoryImpl =
        PropertyUtil.propertyAsString(
            properties, DellProperties.CLIENT_FACTORY, DefaultDellClientFactory.class.getName());
    return loadClientFactory(factoryImpl, properties);
  }

  private static DellClientFactory loadClientFactory(String impl, Map<String, String> properties) {
    DynConstructors.Ctor<DellClientFactory> ctor;
    try {
      ctor = DynConstructors.builder(DellClientFactory.class).hiddenImpl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize DellClientFactory, missing no-arg constructor: %s", impl),
          e);
    }

    DellClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize DellClientFactory, %s does not implement DellClientFactory.",
              impl),
          e);
    }

    factory.initialize(properties);
    return factory;
  }

  static class DefaultDellClientFactory implements DellClientFactory {
    private DellProperties dellProperties;

    DefaultDellClientFactory() {}

    @Override
    public S3Client ecsS3() {
      S3Config config = new S3Config(URI.create(dellProperties.ecsS3Endpoint()));

      config
          .withIdentity(dellProperties.ecsS3AccessKeyId())
          .withSecretKey(dellProperties.ecsS3SecretAccessKey());

      return new S3JerseyClient(config);
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.dellProperties = new DellProperties(properties);
    }
  }
}
