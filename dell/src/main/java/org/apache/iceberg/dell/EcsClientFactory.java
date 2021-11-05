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
import java.util.Optional;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public interface EcsClientFactory {

  /**
   * Create the ECS S3 Client from properties
   */
  static S3Client create(Map<String, String> properties) {
    return createWithFactory(properties).orElseGet(() -> createDefault(properties));
  }

  /**
   * Try to create the ECS S3 client from factory method.
   */
  static Optional<S3Client> createWithFactory(Map<String, String> properties) {
    String factory = properties.get(EcsClientProperties.ECS_CLIENT_FACTORY);
    if (factory == null || factory.isEmpty()) {
      return Optional.empty();
    }

    DynConstructors.Ctor<EcsClientFactory> ctor;
    try {
      ctor = DynConstructors.builder(EcsClientFactory.class).impl(factory).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot find EcsClientFactory implementation %s: %s", factory, e.getMessage()), e);
    }

    EcsClientFactory clientFactory;
    try {
      clientFactory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize EcsClientFactory, %s does not implement EcsClientFactory.", factory), e);
    }

    S3Client client = clientFactory.createS3Client(properties);

    if (client == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid EcsClientFactory %s that return null client",
          factory));
    }

    return Optional.of(client);
  }

  /**
   * Get built-in ECS S3 client.
   */
  static S3Client createDefault(Map<String, String> properties) {
    Preconditions.checkNotNull(properties.get(EcsClientProperties.ENDPOINT),
        "Endpoint (%s) cannot be null", EcsClientProperties.ENDPOINT);
    Preconditions.checkNotNull(properties.get(EcsClientProperties.ACCESS_KEY_ID),
        "Access key (%s) cannot be null", EcsClientProperties.ACCESS_KEY_ID);
    Preconditions.checkNotNull(properties.get(EcsClientProperties.SECRET_ACCESS_KEY),
        "Secret key (%s) cannot be null", EcsClientProperties.SECRET_ACCESS_KEY);

    S3Config config = new S3Config(URI.create(properties.get(EcsClientProperties.ENDPOINT)));

    config.withIdentity(properties.get(EcsClientProperties.ACCESS_KEY_ID))
        .withSecretKey(properties.get(EcsClientProperties.SECRET_ACCESS_KEY));

    return new S3JerseyClient(config);
  }

  S3Client createS3Client(Map<String, String> properties);
}
