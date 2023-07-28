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
package org.apache.iceberg.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class RESTClientBuilder {

  private final Map<String, String> properties;
  private final Map<String, String> baseHeaders = Maps.newHashMap();
  private String uri;
  private ObjectMapper mapper = RESTObjectMapper.mapper();

  RESTClientBuilder(Map<String, String> properties) {
    this.properties = properties;
  }

  public RESTClientBuilder uri(String baseUri) {
    Preconditions.checkNotNull(baseUri, "Invalid uri for http client: null");
    this.uri = RESTUtil.stripTrailingSlash(baseUri);
    return this;
  }

  public RESTClientBuilder withHeader(String key, String value) {
    baseHeaders.put(key, value);
    return this;
  }

  public RESTClientBuilder withHeaders(Map<String, String> headers) {
    baseHeaders.putAll(headers);
    return this;
  }

  public RESTClientBuilder withObjectMapper(ObjectMapper objectMapper) {
    this.mapper = objectMapper;
    return this;
  }

  public RESTClient build() {
    withHeader(RESTClientProperties.CLIENT_VERSION_HEADER, IcebergBuild.fullVersion());
    withHeader(
        RESTClientProperties.CLIENT_GIT_COMMIT_SHORT_HEADER, IcebergBuild.gitCommitShortId());

    RESTClient client;
    if (!properties.containsKey(RESTClientProperties.REST_CLIENT_IMPL)) {
      client = new HTTPClient();
    } else {
      String impl = properties.get(RESTClientProperties.REST_CLIENT_IMPL);
      DynConstructors.Ctor<BaseRESTClient> ctor;
      try {
        ctor =
            DynConstructors.builder(RESTClient.class)
                .loader(RESTClientBuilder.class.getClassLoader())
                .hiddenImpl(impl)
                .buildChecked();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
            String.format("Cannot find no-arg constructor for: %s", impl), e);
      }

      try {
        client = ctor.newInstance();
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot initialize new instance of: %s, not a subclass of RESTClient", impl),
            e);
      }
    }

    client.initialize(uri, mapper, baseHeaders, properties);
    return client;
  }
}
