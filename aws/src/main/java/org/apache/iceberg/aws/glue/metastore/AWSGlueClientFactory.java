/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws.glue.metastore;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.iceberg.aws.glue.util.AWSGlueConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AWSGlueClientFactory implements GlueClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(AWSGlueClientFactory.class);

  private final HiveConf conf;

  public AWSGlueClientFactory(HiveConf conf) {
    Preconditions.checkNotNull(conf, "HiveConf cannot be null");
    this.conf = conf;
  }

  @Override
  public AWSGlue newClient() throws MetaException {
    try {
      AWSGlueClientBuilder glueClientBuilder = AWSGlueClientBuilder.standard()
          .withCredentials(getAWSCredentialsProvider(conf));

      String regionStr = getProperty(AWSGlueConfig.AWS_REGION, conf);
      String glueEndpoint = getProperty(AWSGlueConfig.AWS_GLUE_ENDPOINT, conf);

      // ClientBuilder only allows one of EndpointConfiguration or Region to be set
      if (StringUtils.isNotBlank(glueEndpoint)) {
        LOG.info("Setting glue service endpoint to {}", glueEndpoint);
        glueClientBuilder.setEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(glueEndpoint, null));
      } else if (StringUtils.isNotBlank(regionStr)) {
        LOG.info("Setting region to : {}", regionStr);
        glueClientBuilder.setRegion(regionStr);
      } else {
        Region currentRegion = Regions.getCurrentRegion();
        if (currentRegion != null) {
          LOG.info("Using region from ec2 metadata : {}", currentRegion.getName());
          glueClientBuilder.setRegion(currentRegion.getName());
        } else {
          LOG.info("No region info found, using SDK default region: us-east-1");
        }
      }

      glueClientBuilder.setClientConfiguration(buildClientConfiguration(conf));
      return glueClientBuilder.build();
    } catch (Exception e) {
      LOG.error("Unable to build AWSGlueClient", e);
      throw new MetaException("Unable to build AWSGlueClient: " + e);
    }
  }

  private AWSCredentialsProvider getAWSCredentialsProvider(HiveConf hiveConf) {
    Class<? extends AWSCredentialsProviderFactory> providerFactoryClass = hiveConf
        .getClass(AWSGlueConfig.AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS,
            DefaultAWSCredentialsProviderFactory.class).asSubclass(
            AWSCredentialsProviderFactory.class);
    AWSCredentialsProviderFactory provider = ReflectionUtils.newInstance(
        providerFactoryClass, hiveConf);
    return provider.buildAWSCredentialsProvider(hiveConf);
  }

  private ClientConfiguration buildClientConfiguration(HiveConf hiveConf) {
    ClientConfiguration clientConfiguration = new ClientConfiguration()
        .withMaxErrorRetry(hiveConf.getInt(
            AWSGlueConfig.AWS_GLUE_MAX_RETRY,
            AWSGlueConfig.DEFAULT_MAX_RETRY))
        .withMaxConnections(hiveConf.getInt(
            AWSGlueConfig.AWS_GLUE_MAX_CONNECTIONS,
            AWSGlueConfig.DEFAULT_MAX_CONNECTIONS))
        .withConnectionTimeout(hiveConf.getInt(
            AWSGlueConfig.AWS_GLUE_CONNECTION_TIMEOUT,
            AWSGlueConfig.DEFAULT_CONNECTION_TIMEOUT))
        .withSocketTimeout(hiveConf.getInt(
            AWSGlueConfig.AWS_GLUE_SOCKET_TIMEOUT,
            AWSGlueConfig.DEFAULT_SOCKET_TIMEOUT));
    return clientConfiguration;
  }

  private static String getProperty(String propertyName, HiveConf conf) {
    return Strings.isNullOrEmpty(System.getProperty(propertyName)) ?
        conf.get(propertyName) : System.getProperty(propertyName);
  }
}
