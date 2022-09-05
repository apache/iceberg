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

import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Takes in the full configuration for the {@link RESTSessionCatalog}, which should already have
 * called the server's initial configuration route. Using the merged configuration, an instance of
 * {@link RESTClient} is obtained that can be used with the RESTCatalog.
 */
public class HTTPClientFactory implements Function<Map<String, String>, RESTClient> {

  @VisibleForTesting static final String CLIENT_VERSION_HEADER = "X-Client-Version";

  @VisibleForTesting
  static final String CLIENT_GIT_COMMIT_SHORT_HEADER = "X-Client-Git-Commit-Short";

  @Override
  public RESTClient apply(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Invalid configuration: null");
    Preconditions.checkArgument(
        properties.containsKey(CatalogProperties.URI), "REST Catalog server URI is required");

    String baseURI = properties.get(CatalogProperties.URI).trim();
    String clientVersion = IcebergBuild.fullVersion();
    String gitCommitShortId = IcebergBuild.gitCommitShortId();

    return HTTPClient.builder()
        .withHeader(CLIENT_VERSION_HEADER, clientVersion)
        .withHeader(CLIENT_GIT_COMMIT_SHORT_HEADER, gitCommitShortId)
        .uri(baseURI)
        .build();
  }
}
