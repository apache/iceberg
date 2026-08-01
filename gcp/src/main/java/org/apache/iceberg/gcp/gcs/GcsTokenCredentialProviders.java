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
package org.apache.iceberg.gcp.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.PropertyUtil;

public class GcsTokenCredentialProviders {

  private static final DefaultGcsTokenCredentialProvider DEFAULT_PROVIDER =
      new DefaultGcsTokenCredentialProvider();

  private GcsTokenCredentialProviders() {}

  public static GcsTokenCredentialProvider defaultFactory() {
    return DEFAULT_PROVIDER;
  }

  public static GcsTokenCredentialProvider from(Map<String, String> properties) {
    String providerImpl =
        PropertyUtil.propertyAsString(
            properties, GCPProperties.GCS_TOKEN_CREDENTIAL_PROVIDER, null);
    Map<String, String> credentialProviderProperties =
        PropertyUtil.propertiesWithPrefix(properties, GCPProperties.GCS_TOKEN_PROVIDER_PREFIX);
    return loadCredentialProvider(providerImpl, credentialProviderProperties);
  }

  private static GcsTokenCredentialProvider loadCredentialProvider(
      String impl, Map<String, String> properties) {
    if (Strings.isNullOrEmpty(impl)) {
      GcsTokenCredentialProvider provider = defaultFactory();
      provider.initialize(properties);
      return provider;
    }

    DynConstructors.Ctor<GcsTokenCredentialProvider> ctor;
    try {
      ctor =
          DynConstructors.builder(GcsTokenCredentialProvider.class)
              .loader(GcsTokenCredentialProviders.class.getClassLoader())
              .hiddenImpl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize GcsTokenCredentialProvider, missing no-arg constructor: %s", impl),
          e);
    }

    GcsTokenCredentialProvider provider;
    try {
      provider = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize GcsTokenCredentialProvider, %s does not implement GcsTokenCredentialProvider.",
              impl),
          e);
    }

    provider.initialize(properties);
    return provider;
  }

  static class DefaultGcsTokenCredentialProvider implements GcsTokenCredentialProvider {

    @Override
    public GoogleCredentials credential() {
      try {
        return GoogleCredentials.getApplicationDefault();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to get application default GCS credentials", e);
      }
    }

    @Override
    public void initialize(Map<String, String> properties) {}
  }
}
