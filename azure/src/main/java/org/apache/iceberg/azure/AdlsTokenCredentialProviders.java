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
package org.apache.iceberg.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.PropertyUtil;

public class AdlsTokenCredentialProviders {

  private static final DefaultTokenCredentialProvider DEFAULT_TOKEN_CREDENTIAL_PROVIDER =
      new DefaultTokenCredentialProvider();

  private AdlsTokenCredentialProviders() {}

  public static AdlsTokenCredentialProvider defaultFactory() {
    return DEFAULT_TOKEN_CREDENTIAL_PROVIDER;
  }

  public static AdlsTokenCredentialProvider from(Map<String, String> properties) {
    String providerImpl =
        PropertyUtil.propertyAsString(
            properties, AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER, null);
    Map<String, String> credentialProviderProperties =
        PropertyUtil.propertiesWithPrefix(properties, AzureProperties.ADLS_TOKEN_PROVIDER_PREFIX);
    return loadCredentialProvider(providerImpl, credentialProviderProperties);
  }

  private static AdlsTokenCredentialProvider loadCredentialProvider(
      String impl, Map<String, String> properties) {
    if (Strings.isNullOrEmpty(impl)) {
      AdlsTokenCredentialProvider provider = defaultFactory();
      provider.initialize(properties);
      return provider;
    }

    DynConstructors.Ctor<AdlsTokenCredentialProvider> ctor;
    try {
      ctor =
          DynConstructors.builder(AdlsTokenCredentialProvider.class)
              .loader(AdlsTokenCredentialProviders.class.getClassLoader())
              .hiddenImpl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AdlsTokenCredentialProvider, missing no-arg constructor: %s",
              impl),
          e);
    }

    AdlsTokenCredentialProvider provider;
    try {
      provider = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AdlsTokenCredentialProvider, %s does not implement AdlsTokenCredentialProvider.",
              impl),
          e);
    }

    provider.initialize(properties);
    return provider;
  }

  static class DefaultTokenCredentialProvider implements AdlsTokenCredentialProvider {

    @Override
    public TokenCredential credential() {
      return new DefaultAzureCredentialBuilder().build();
    }

    @Override
    public void initialize(Map<String, String> properties) {}
  }
}
