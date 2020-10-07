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

package org.apache.iceberg.aws.glue.util;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.aws.glue.metastore.GlueClientFactory;

public final class GlueTestClientFactory implements GlueClientFactory {

  private static final int SC_GATEWAY_TIMEOUT = 504;

  @Override
  public AWSGlue newClient() throws MetaException {
    AWSGlueClientBuilder glueClientBuilder = AWSGlueClientBuilder.standard()
        .withClientConfiguration(createGatewayTimeoutRetryableConfiguration())
        .withCredentials(new DefaultAWSCredentialsProviderChain());

    String endpoint = System.getProperty("endpoint");
    if (StringUtils.isNotBlank(endpoint)) {
      glueClientBuilder.setEndpointConfiguration(new EndpointConfiguration(endpoint, null));
    }

    return glueClientBuilder.build();
  }

  private static ClientConfiguration createGatewayTimeoutRetryableConfiguration() {
    ClientConfiguration retryableConfig = new ClientConfiguration();
    RetryPolicy.RetryCondition retryCondition = new PredefinedRetryPolicies.SDKDefaultRetryCondition() {
      @Override
      public boolean shouldRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception,
                                 int retriesAttempted) {
        if (super.shouldRetry(originalRequest, exception, retriesAttempted)) {
          return true;
        }
        if (exception != null && exception instanceof AmazonServiceException) {
          AmazonServiceException ase = (AmazonServiceException) exception;
          if (ase.getStatusCode() == SC_GATEWAY_TIMEOUT) {
            return true;
          }
        }
        return false;
      }
    };
    RetryPolicy retryPolicy = new RetryPolicy(retryCondition, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
                                                     PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY, true);
    retryableConfig.setRetryPolicy(retryPolicy);
    return retryableConfig;
  }

}
