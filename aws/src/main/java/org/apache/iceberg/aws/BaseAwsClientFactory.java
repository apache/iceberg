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
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

abstract class BaseAwsClientFactory implements AwsClientFactory {
  private String httpClientConfig;

  /**
   * Set http client config from properties map
   *
   * @param properties catalog properties
   */
  protected void setHttpClientConfig(Map<String, String> properties) {
    httpClientConfig = PropertyUtil.propertyAsString(
        properties, TableProperties.HTTP_CLIENT_TYPE, TableProperties.HTTP_CLIENT_TYPE_DEFAULT);

    ValidationException.check(
        (httpClientConfig.equalsIgnoreCase(TableProperties.URL_HTTP_CLIENT) ||
            httpClientConfig.equalsIgnoreCase(TableProperties.APACHE_HTTP_CLIENT)),
        "HTTP client can be either url or apache");
  }

  /**
   * Create a http client builder based on client config
   */
  public SdkHttpClient.Builder getHttpClientBuilder() {
    if (getHttpClientConfig().equalsIgnoreCase(TableProperties.APACHE_HTTP_CLIENT)) {
      return ApacheHttpClient.builder();
    } else {
      return UrlConnectionHttpClient.builder();
    }
  }

  /**
   * Get http client config
   */
  public String getHttpClientConfig() {
    return this.httpClientConfig;
  }
}
