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

public class AzureTokenCredentialProviders {

  private static final DefaultTokenCredentialProvider DEFAULT_TOKEN_CREDENTIAL_PROVIDER =
      new DefaultTokenCredentialProvider();

  private AzureTokenCredentialProviders() {}

  public static AzureTokenCredentialProvider defaultFactory() {
    return DEFAULT_TOKEN_CREDENTIAL_PROVIDER;
  }

  public static AzureTokenCredentialProvider from(Map<String, String> properties) {
    String providerImpl =
        PropertyUtil.propertyAsString(
            properties, AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER, null);
    return loadCredentialProvider(providerImpl, properties);
  }

  private static AzureTokenCredentialProvider loadCredentialProvider(
      String impl, Map<String, String> properties) {
    if (Strings.isNullOrEmpty(impl)) {
      AzureTokenCredentialProvider provider = defaultFactory();
      provider.initialize(properties);
      return provider;
    }

    DynConstructors.Ctor<AzureTokenCredentialProvider> ctor;
    try {
      ctor =
          DynConstructors.builder(AzureTokenCredentialProvider.class)
              .loader(AzureTokenCredentialProviders.class.getClassLoader())
              .hiddenImpl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AzureTokenCredentialProvider, missing no-arg constructor: %s",
              impl),
          e);
    }

    AzureTokenCredentialProvider provider;
    try {
      provider = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AzureTokenCredentialProvider, %s does not implement AzureTokenCredentialProvider.",
              impl),
          e);
    }

    provider.initialize(properties);
    return provider;
  }

  static class DefaultTokenCredentialProvider implements AzureTokenCredentialProvider {

    @Override
    public TokenCredential credential() {
      return new DefaultAzureCredentialBuilder().build();
    }

    @Override
    public void initialize(Map<String, String> properties) {}
  }
}
