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
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;

public class AwsClientProperties {
  /**
   * Configure the AWS credentials provider used to create AWS clients. A fully qualified concrete
   * class with package that implements the {@link AwsCredentialsProvider} interface is required.
   *
   * <p>Additionally, the implementation class must also have a create() or create(Map) method
   * implemented, which returns an instance of the class that provides aws credentials provider.
   *
   * <p>Example:
   * client.credentials-provider=software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
   *
   * <p>When set, the default client factory {@link
   * org.apache.iceberg.aws.AwsClientFactories#defaultFactory()} and other AWS client factory
   * classes will use this provider to get AWS credentials provided instead of reading the default
   * credential chain to get AWS access credentials.
   */
  public static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";

  /**
   * Used by the client.credentials-provider configured value that will be used by {@link
   * org.apache.iceberg.aws.AwsClientFactories#defaultFactory()} and other AWS client factory
   * classes to pass provider-specific properties. Each property consists of a key name and an
   * associated value.
   */
  private static final String CLIENT_CREDENTIAL_PROVIDER_PREFIX = "client.credentials-provider.";

  /**
   * Used by {@link org.apache.iceberg.aws.AwsClientFactories.DefaultAwsClientFactory} and also
   * other client factory classes. If set, all AWS clients except STS client will use the given
   * region instead of the default region chain.
   */
  public static final String CLIENT_REGION = "client.region";

  private String clientRegion;
  private String clientCredentialsProvider;
  private final Map<String, String> clientCredentialsProviderProperties;

  public AwsClientProperties() {
    this.clientRegion = null;
    this.clientCredentialsProvider = null;
    this.clientCredentialsProviderProperties = null;
  }

  public AwsClientProperties(Map<String, String> properties) {
    this.clientRegion = properties.get(CLIENT_REGION);
    this.clientCredentialsProvider = properties.get(CLIENT_CREDENTIALS_PROVIDER);
    this.clientCredentialsProviderProperties =
        PropertyUtil.propertiesWithPrefix(properties, CLIENT_CREDENTIAL_PROVIDER_PREFIX);
  }

  public String clientRegion() {
    return clientRegion;
  }

  public void setClientRegion(String clientRegion) {
    this.clientRegion = clientRegion;
  }

  /**
   * Configure a client AWS region.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyClientRegionConfiguration)
   * </pre>
   */
  public <T extends AwsClientBuilder> void applyClientRegionConfiguration(T builder) {
    if (clientRegion != null) {
      builder.region(Region.of(clientRegion));
    }
  }

  /**
   * Returns a credentials provider instance. If params were set, we return a new credentials
   * instance. If none of the params are set, we try to dynamically load the provided credentials
   * provider class. Upon loading the class, we try to invoke {@code create(Map<String, String>)}
   * static method. If that fails, we fall back to {@code create()}. If credential provider class
   * wasn't set, we fall back to default credentials provider.
   *
   * @param accessKeyId the AWS access key ID
   * @param secretAccessKey the AWS secret access key
   * @param sessionToken the AWS session token
   * @return a credentials provider instance
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public AwsCredentialsProvider credentialsProvider(
      String accessKeyId, String secretAccessKey, String sessionToken) {
    if (!Strings.isNullOrEmpty(accessKeyId) && !Strings.isNullOrEmpty(secretAccessKey)) {
      if (Strings.isNullOrEmpty(sessionToken)) {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId, secretAccessKey));
      } else {
        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
      }
    }

    if (!Strings.isNullOrEmpty(this.clientCredentialsProvider)) {
      return credentialsProvider(this.clientCredentialsProvider);
    }

    return DefaultCredentialsProvider.create();
  }

  private AwsCredentialsProvider credentialsProvider(String credentialsProviderClass) {
    Class<?> providerClass;
    try {
      providerClass = DynClasses.builder().impl(credentialsProviderClass).buildChecked();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot load class %s, it does not exist in the classpath", credentialsProviderClass),
          e);
    }

    Preconditions.checkArgument(
        AwsCredentialsProvider.class.isAssignableFrom(providerClass),
        String.format(
            "Cannot initialize %s, it does not implement %s.",
            credentialsProviderClass, AwsCredentialsProvider.class.getName()));

    try {
      return createCredentialsProvider(providerClass);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create an instance of %s, it does not contain a static 'create' or 'create(Map<String, String>)' method",
              credentialsProviderClass),
          e);
    }
  }

  private AwsCredentialsProvider createCredentialsProvider(Class<?> providerClass)
      throws NoSuchMethodException {
    AwsCredentialsProvider provider;
    try {
      provider =
          DynMethods.builder("create")
              .hiddenImpl(providerClass, Map.class)
              .buildStaticChecked()
              .invoke(clientCredentialsProviderProperties);
    } catch (NoSuchMethodException e) {
      provider =
          DynMethods.builder("create").hiddenImpl(providerClass).buildStaticChecked().invoke();
    }
    return provider;
  }
}
