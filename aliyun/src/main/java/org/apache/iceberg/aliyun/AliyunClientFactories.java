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

import com.aliyun.credentials.models.CredentialModel;
import com.aliyun.credentials.provider.OIDCRoleArnCredentialProvider;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.BasicCredentials;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(DefaultAliyunClientFactory.class);
    private AliyunProperties aliyunProperties;

    DefaultAliyunClientFactory() {}

    /**
     * Check if RRSA environment variables are present. RRSA requires
     * ALIBABA_CLOUD_OIDC_PROVIDER_ARN, ALIBABA_CLOUD_ROLE_ARN and ALIBABA_CLOUD_OIDC_TOKEN_FILE to
     * be set.
     */
    boolean isRrsaEnvironmentAvailable() {
      String oidcProviderArn = System.getenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN");
      String roleArn = System.getenv("ALIBABA_CLOUD_ROLE_ARN");
      String oidcTokenFile = System.getenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE");
      return oidcProviderArn != null
          && !oidcProviderArn.isEmpty()
          && roleArn != null
          && !roleArn.isEmpty()
          && oidcTokenFile != null
          && !oidcTokenFile.isEmpty();
    }

    @Override
    public OSS newOSSClient() {
      Preconditions.checkNotNull(
          aliyunProperties,
          "Cannot create aliyun oss client before initializing the AliyunClientFactory.");

      String endpoint = aliyunProperties.ossEndpoint();

      // Check if RRSA environment is available
      if (isRrsaEnvironmentAvailable()) {
        try {
          LOG.info(
              "Detected RRSA environment variables, creating OSS client with RRSA credentials for endpoint: {}",
              endpoint);

          // Use OIDCRoleArnCredentialProvider directly with built-in caching and auto-refresh
          final OIDCRoleArnCredentialProvider oidcProvider =
              OIDCRoleArnCredentialProvider.builder().build();

          CredentialsProvider ossCredProvider =
              new CredentialsProvider() {
                private volatile Credentials currentCredentials;

                @Override
                public void setCredentials(Credentials credentials) {}

                @Override
                public Credentials getCredentials() {
                  try {
                    LOG.debug("Getting credentials using RRSA");
                    // getCredentials() returns cached credentials and auto-refreshes when needed
                    CredentialModel cred = oidcProvider.getCredentials();
                    long expirationSeconds = 0;
                    if (cred.getExpiration() > 0) {
                      expirationSeconds =
                          (cred.getExpiration() - System.currentTimeMillis()) / 1000;
                    }
                    this.currentCredentials =
                        new BasicCredentials(
                            cred.getAccessKeyId(),
                            cred.getAccessKeySecret(),
                            cred.getSecurityToken(),
                            expirationSeconds);
                    return this.currentCredentials;
                  } catch (Exception e) {
                    throw new RuntimeException("Failed to get RRSA credentials", e);
                  }
                }
              };
          return new OSSClientBuilder().build(endpoint, ossCredProvider);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create RRSA OSS client", e);
        }
      } else if (Strings.isNullOrEmpty(aliyunProperties.securityToken())) {
        return new OSSClientBuilder()
            .build(
                aliyunProperties.ossEndpoint(),
                aliyunProperties.accessKeyId(),
                aliyunProperties.accessKeySecret());
      } else {
        return new OSSClientBuilder()
            .build(
                aliyunProperties.ossEndpoint(),
                aliyunProperties.accessKeyId(),
                aliyunProperties.accessKeySecret(),
                aliyunProperties.securityToken());
      }
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
